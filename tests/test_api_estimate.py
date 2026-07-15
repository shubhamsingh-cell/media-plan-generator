"""Regression tests for POST /api/estimate (single-sourced plan estimates).

Bug fixed: the wizard's "Projected outcomes" preview panel
(templates/partials/index/body_preview_js.html) used to run its OWN flat
effective-CPA JS formula, independent of the real engine. On the
Manpower-AmeriGas brief ($150,000 budget, "Logistics & Supply Chain",
"CDL A Driver", 250-hire target, 6 US markets, 6 channels) that formula
showed ~617 estimated hires / $12.15-12.67 cost-per-application, while
``budget_engine.calculate_budget_allocation`` -- the SAME engine
``/api/generate`` calls -- produces 48 projected hires at $3,125 CPH for
that brief. A 13x overpromise, verified on prod 2026-07-12.

``app._compute_plan_estimate`` (app.py, right after
``_split_city_state_country``) fixes this by reusing
``calculate_budget_allocation`` directly, mirroring the exact input
construction the sync ``/api/generate`` handler performs (industry
classification -> INDUSTRY_ALLOC_PROFILES channel split -> role/location
dicts) before calling the engine, with ``synthesized_data=None`` (no
enrichment -- this is a fast, synchronous preview call).

This file covers:
    1. Parity -- for 3+ industry/budget combos, ``_compute_plan_estimate``'s
       est_hires/est_cph match ``calculate_budget_allocation``'s
       ``total_projected`` for the identical inputs it constructs
       internally, within rounding.
    2. Validation -- malformed/insufficient briefs raise
       ``_EstimateValidationError`` (mapped to a 4xx at the route, never a
       500) instead of raising an unhandled exception or silently
       returning nonsense numbers. Includes the 4xx-contract-violation
       hardening: non-numeric ``count``/``campaign_start_month`` (e.g.
       "abc"/"x"), non-finite JSON floats (NaN/Infinity, which
       ``json.loads()`` parses without complaint) for budget/count/month,
       and negative budgets (numeric or string, e.g. "-$50,000" -- the
       string form used to have its sign silently stripped by
       ``parse_budget``'s regex extractor instead of being rejected).
    3. Wiring -- source-inspection checks (matching the established
       pattern in test_app_vendor_gate_wiring.py / test_routes.py, since
       app.py's request handler is not a standalone testable unit) that
       the ``/api/estimate`` route exists, copies the ``/api/generate``
       auth block, is CSRF-exempt via the same-origin auth (not a bare
       CSRF bypass), and uses its OWN rate limiter isolated from
       ``/api/generate``'s 10 req/min quota. Also source-inspects
       ``templates/partials/index/body_preview_js.html``'s debounced
       refetch gate (``fetchEstimate()``'s ``sig``) to guard against a
       stale preview on industry/client-name change (Finding 1).
    4. Live E2E (skipped when no server is reachable, matching
       tests/test_e2e.py's convention) -- unauthenticated requests get
       401, malformed JSON gets 4xx, and a real request against the
       Manpower brief returns the exact prod-verified 48 hires / $3,125
       CPH.
    5. Same-origin gate hardening (2026-07-15 adversarial review) -- the
       S48 gate on /api/estimate AND /api/generate used to be a substring
       test (``"localhost" in (Origin or Referer)``), bypassable with
       ``Referer: https://evil.com/?x=localhost`` or
       ``Origin: https://localhost.evil.com``. It is now parsed-host
       equality via ``MediaPlanHandler._check_same_origin_auth`` against
       ``app._SAME_ORIGIN_ALLOWED_HOSTS``. Covered three ways: helper
       unit tests, source-inspection wiring checks on both gates, and
       live HTTP tests against an in-process ``app.ThreadedHTTPServer``
       on an ephemeral port (always runs -- no external server needed,
       matching test_generate_concurrency.py's fixture pattern).

Runs under pytest, or standalone:
``python3 tests/test_api_estimate.py``.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import threading
import time
import types
from pathlib import Path
from typing import Any, Iterator, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402
import budget_engine  # noqa: E402
from kb_loader import load_knowledge_base  # noqa: E402
from ppt_generator import INDUSTRY_ALLOC_PROFILES  # noqa: E402
from shared_utils import parse_budget  # noqa: E402


# ---------------------------------------------------------------------------
# Shared brief fixtures (3+ industry/budget combos, per task brief)
# ---------------------------------------------------------------------------

MANPOWER_BRIEF: dict[str, Any] = {
    "budget": "$150,000",
    "industry": "Logistics & Supply Chain",
    "client_name": "Manpower - Amerigas",
    "roles": ["CDL A Driver"],
    "locations": [
        "Massachusetts",
        "Maine",
        "New Hampshire",
        "Rhode Island",
        "Connecticut",
        "Denver, CO",
    ],
}

HEALTHCARE_BRIEF: dict[str, Any] = {
    "budget": "$50,000",
    "industry": "Healthcare & Medical",
    "client_name": "Atria Senior Living",
    "roles": ["Nurse"],
    "locations": ["New York, NY"],
}

RETAIL_BRIEF: dict[str, Any] = {
    "budget": "$300,000",
    "industry": "Retail",
    "client_name": "BigBox Retail",
    "roles": ["Store Associate", "Shift Lead"],
    "locations": ["Chicago, IL", "Dallas, TX"],
}

PARITY_BRIEFS: list[tuple[str, dict[str, Any]]] = [
    ("manpower_logistics_150k", MANPOWER_BRIEF),
    ("healthcare_50k", HEALTHCARE_BRIEF),
    ("retail_300k", RETAIL_BRIEF),
]


def _reference_total_projected(brief: dict[str, Any]) -> dict[str, Any]:
    """Call calculate_budget_allocation directly, building inputs the same
    way _compute_plan_estimate does, as an independent parity oracle."""
    roles_titles = [str(r) for r in brief["roles"]]
    industry_profile = app.classify_industry(
        brief["industry"], brief["client_name"], roles_titles
    )
    industry_key = industry_profile.get("legacy_key", "general_entry_level")

    default_alloc = {
        "programmatic_dsp": 35,
        "global_boards": 20,
        "niche_boards": 15,
        "social_media": 12,
        "regional_boards": 8,
        "employer_branding": 5,
        "apac_regional": 3,
        "emea_regional": 2,
    }
    channel_pcts = dict(INDUSTRY_ALLOC_PROFILES.get(industry_key, default_alloc))
    intl_pct = channel_pcts.pop("apac_regional", 0) + channel_pcts.pop(
        "emea_regional", 0
    )
    if intl_pct > 0:
        top_ch = max(channel_pcts, key=lambda k: channel_pcts[k])
        channel_pcts[top_ch] += intl_pct

    roles_for_ba = [{"title": t, "count": 1, "tier": "Professional"} for t in roles_titles]
    locs_for_ba = [app._split_city_state_country(loc) for loc in brief["locations"]]

    result = budget_engine.calculate_budget_allocation(
        total_budget=parse_budget(brief["budget"]),
        roles=roles_for_ba,
        locations=locs_for_ba,
        industry=industry_key,
        channel_percentages=channel_pcts,
        synthesized_data=None,
        knowledge_base=load_knowledge_base(),
        collar_type="",
        campaign_start_month=0,
    )
    return result.get("total_projected", {}) if isinstance(result, dict) else {}


# ---------------------------------------------------------------------------
# 1. Parity: /api/estimate's numbers == calculate_budget_allocation's
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name,brief", PARITY_BRIEFS, ids=[n for n, _ in PARITY_BRIEFS])
def test_estimate_matches_budget_engine_total_projected(
    name: str, brief: dict[str, Any]
) -> None:
    """est_hires/est_cph must equal calculate_budget_allocation's
    total_projected hires/cost_per_hire for the identical inputs, within
    rounding -- this is the single-sourcing guarantee the fix exists for."""
    est = app._compute_plan_estimate(dict(brief))
    reference = _reference_total_projected(brief)

    assert est["est_hires"] == int(reference.get("hires") or 0), (
        f"{name}: est_hires {est['est_hires']} != engine hires "
        f"{reference.get('hires')}"
    )
    assert est["est_cph"] == pytest.approx(
        float(reference.get("cost_per_hire") or 0.0), abs=0.01
    ), f"{name}: est_cph {est['est_cph']} != engine cost_per_hire {reference.get('cost_per_hire')}"
    assert est["est_applications"] == int(reference.get("applications") or 0), (
        f"{name}: est_applications {est['est_applications']} != engine "
        f"applications {reference.get('applications')}"
    )


def test_manpower_brief_reproduces_prod_incident_numbers() -> None:
    """The exact numbers verified on prod 2026-07-12: 48 hires at $3,125
    CPH for the Manpower-AmeriGas brief -- NOT the naive JS estimator's
    ~617 hires. Locks in the root-cause fix against regression."""
    est = app._compute_plan_estimate(dict(MANPOWER_BRIEF))
    assert est["est_hires"] == 48
    assert est["est_cph"] == pytest.approx(3125.0, abs=0.01)


# ---------------------------------------------------------------------------
# 2. Validation: malformed/insufficient briefs -> _EstimateValidationError
# ---------------------------------------------------------------------------


class TestValidation:
    """_compute_plan_estimate must raise _EstimateValidationError (mapped
    to a 4xx at the route) on malformed input -- never a bare exception,
    never a silently-wrong number."""

    def test_missing_budget_raises(self) -> None:
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate({"roles": ["Driver"]})

    def test_unparseable_budget_raises_not_silently_defaulted(self) -> None:
        """shared_utils.parse_budget() silently falls back to a $100,000
        default (with parse_budget.last_was_defaulted=True) when it can't
        parse a budget string like "$0" -- exactly the kind of silent
        not-what-the-user-typed number this endpoint exists to eliminate.
        _compute_plan_estimate must check the defaulted flag and raise
        rather than quietly returning an estimate for a budget nobody
        entered."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate({"budget": "$0", "roles": ["Driver"]})

    def test_no_roles_raises(self) -> None:
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate({"budget": "$50,000"})

    def test_empty_roles_list_raises(self) -> None:
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate({"budget": "$50,000", "roles": []})

    def test_roles_not_a_list_raises(self) -> None:
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate({"budget": "$50,000", "roles": "Driver"})

    def test_locations_not_a_list_raises(self) -> None:
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {"budget": "$50,000", "roles": ["Driver"], "locations": "NYC"}
            )

    def test_blank_role_strings_are_ignored_not_crashed_on(self) -> None:
        """Whitespace-only role entries are skipped; if nothing is left,
        that's still a validation error, not a KeyError/IndexError."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate({"budget": "$50,000", "roles": ["   ", ""]})

    def test_valid_minimal_brief_does_not_raise(self) -> None:
        """Sanity check: a well-formed minimal brief (no locations) still
        computes -- locations are optional (calculate_budget_allocation
        degrades gracefully to a default cost multiplier)."""
        est = app._compute_plan_estimate(
            {"budget": "$50,000", "roles": ["Warehouse Associate"]}
        )
        assert est["est_hires"] >= 0
        assert est["est_cph"] >= 0

    # -- 4xx-contract hardening: a malformed/hostile client value must
    #    raise _EstimateValidationError (-> 400), never let a bare
    #    ValueError/TypeError/OverflowError escape as an unhandled 500. --

    def test_non_numeric_role_count_raises_not_500(self) -> None:
        """count="abc" used to hit a bare int("abc") and raise an
        unhandled ValueError, which the route only catches generically
        and turns into a 500 -- a client-input error must be a 400."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {
                    "budget": "$50,000",
                    "roles": [{"title": "Driver", "count": "abc"}],
                }
            )

    def test_non_numeric_campaign_start_month_raises_not_500(self) -> None:
        """campaign_start_month="x" used to hit a bare int("x") and raise
        an unhandled ValueError -- same 4xx-contract violation as the
        role-count case above."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {
                    "budget": "$50,000",
                    "roles": ["Driver"],
                    "campaign_start_month": "x",
                }
            )

    def test_nan_budget_raises(self) -> None:
        """A bare NaN token in the JSON body (json.loads() parses it into
        a Python float('nan') without complaint) must be rejected as a
        validation error, not silently propagated into the engine."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {"budget": float("nan"), "roles": ["Driver"]}
            )

    def test_infinite_budget_raises(self) -> None:
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {"budget": float("inf"), "roles": ["Driver"]}
            )

    def test_nan_role_count_raises(self) -> None:
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {
                    "budget": "$50,000",
                    "roles": [{"title": "Driver", "count": float("nan")}],
                }
            )

    def test_infinite_campaign_start_month_raises(self) -> None:
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {
                    "budget": "$50,000",
                    "roles": ["Driver"],
                    "campaign_start_month": float("inf"),
                }
            )

    def test_negative_numeric_budget_raises(self) -> None:
        """A raw negative JSON number for budget must be rejected outright
        -- calculate_budget_allocation has no concept of a negative
        campaign spend."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate({"budget": -50000, "roles": ["Driver"]})

    def test_negative_string_budget_raises(self) -> None:
        """shared_utils.parse_budget()'s regex-based number extractor
        strips a leading "-" when pulling digits out of a string (e.g.
        "-$50,000" -> 50000), which would otherwise silently flip a
        negative budget positive instead of rejecting it."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {"budget": "-$50,000", "roles": ["Driver"]}
            )

    def test_exact_adversarial_repro_body_raises(self) -> None:
        """The EXACT body from the 2026-07-15 adversarial review that
        returned a 500 on prod-shaped input: count="abc" hit the bare
        int() at the role-dict branch and escaped as an unhandled
        ValueError. Locked in verbatim so the repro can never regress."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {
                    "budget": "$150,000",
                    "roles": [{"title": "X", "count": "abc"}],
                    "locations": ["Dallas, TX"],
                }
            )

    def test_list_role_count_raises_not_500(self) -> None:
        """count=[1,2] used to raise TypeError from int([1, 2]) -- the
        non-string flavor of the same unguarded-cast crash class."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {
                    "budget": "$150,000",
                    "roles": [{"title": "X", "count": [1, 2]}],
                    "locations": ["Dallas, TX"],
                }
            )

    def test_month_name_campaign_start_month_raises_not_500(self) -> None:
        """campaign_start_month="March" (a plausible client value, not
        just garbage) used to raise ValueError from int("March")."""
        with pytest.raises(app._EstimateValidationError):
            app._compute_plan_estimate(
                {
                    "budget": "$150,000",
                    "roles": [{"title": "X", "count": 1}],
                    "locations": ["Dallas, TX"],
                    "campaign_start_month": "March",
                }
            )


# ---------------------------------------------------------------------------
# 3. Wiring: source-inspection checks (app.py's handler is not a
#    standalone testable unit -- matches test_app_vendor_gate_wiring.py /
#    test_routes.py's established pattern for this codebase).
# ---------------------------------------------------------------------------


class TestRouteWiring:
    """Verify /api/estimate is registered with the same auth posture as
    /api/generate but an ISOLATED rate-limit quota."""

    def test_route_exists(self, app_source: str) -> None:
        assert 'path == "/api/estimate"' in app_source

    def test_route_checks_joveo_or_admin_or_origin_auth(self, app_source: str) -> None:
        """Auth block must be present near the route (shared with
        /api/generate -- same-origin + @joveo.com, S48). Same-origin is
        now the parsed-host helper (_check_same_origin_auth), NOT the old
        bypassable substring test.

        Anchored on the route-handler's own comment rather than
        'if path == "/api/estimate":' -- that substring also matches
        inside 'elif path == "/api/estimate":' in the earlier rate-limiter
        dispatcher, which would find the wrong occurrence first.
        """
        idx = app_source.index(
            "# ── Single-sourced plan estimate for the wizard's live preview ──"
        )
        snippet = app_source[idx : idx + 2000]
        assert "_check_joveo_auth" in snippet
        assert "_check_admin_auth" in snippet
        assert "_check_same_origin_auth" in snippet
        assert "AUTH_REQUIRED" in snippet

    def test_both_gates_use_parsed_host_helper_not_substring(
        self, app_source: str
    ) -> None:
        """Neither /api/estimate's nor /api/generate's auth gate may
        regress to the pre-2026-07-15 substring test
        (``"localhost" in (Origin or Referer)``), which
        ``Referer: https://evil.com/?x=localhost`` satisfied. Both must
        call the ONE shared helper. Scoped to the two gate snippets only
        -- /api/chat and /api/chat/stream still carry the old pattern and
        are deliberately out of this fix's scope."""
        est_idx = app_source.index(
            "# ── Single-sourced plan estimate for the wizard's live preview ──"
        )
        gen_idx = app_source.index(
            "S48: Same-origin + @joveo.com auth for plan generation"
        )
        for name, snippet in (
            ("estimate", app_source[est_idx : est_idx + 1200]),
            ("generate", app_source[gen_idx : gen_idx + 1200]),
        ):
            assert "_check_same_origin_auth" in snippet, (
                f"/api/{name} gate must call the shared parsed-host helper"
            )
            assert 'in (self.headers.get("Origin")' not in snippet, (
                f"/api/{name} gate must not use the bypassable substring check"
            )

    def test_allowed_hosts_are_exactly_the_s48_set(self) -> None:
        """The parsed-host allowlist must be exactly the three S48 hosts
        -- no additions (scope creep) and no removals (would break the
        prod site, nova.joveo.com embeds, or local dev)."""
        assert app._SAME_ORIGIN_ALLOWED_HOSTS == frozenset(
            {
                "media-plan-generator.onrender.com",
                "nova.joveo.com",
                "localhost",
            }
        )

    def test_route_has_isolated_rate_limiter(self, app_source: str) -> None:
        """/api/estimate must use its OWN RateLimiter instance, not
        _rl_generate (which /api/generate uses at 10 req/min) and not
        silently fall through to _rl_general."""
        assert "_rl_estimate = RateLimiter()" in app_source
        idx = app_source.index('elif path == "/api/estimate":')
        snippet = app_source[idx : idx + 300]
        assert "_rl_estimate.is_allowed" in snippet

    def test_route_is_csrf_exempt_like_generate(self, app_source: str) -> None:
        """Same-origin + @joveo.com auth substitutes for CSRF (matches
        /api/generate's S48 rationale) -- the debounced preview fetch has
        no CSRF-token bootstrap dependency."""
        idx = app_source.index("_CSRF_EXEMPT_PATHS = (")
        snippet = app_source[idx : idx + 1500]
        assert '"/api/estimate"' in snippet

    def test_compute_plan_estimate_reuses_calculate_budget_allocation(
        self, app_source: str
    ) -> None:
        """The endpoint must call the real engine, not a parallel formula."""
        idx = app_source.index("def _compute_plan_estimate(")
        end_idx = app_source.index("\ndef ", idx + 1)
        snippet = app_source[idx:end_idx]
        assert "calculate_budget_allocation(" in snippet
        assert "synthesized_data=None" in snippet


@pytest.fixture(scope="module")
def preview_js_source() -> str:
    """Read templates/partials/index/body_preview_js.html (cached for the
    module) -- the wizard's debounced /api/estimate client, source-
    inspected below since it has no standalone JS test runner (matches
    TestRouteWiring's source-inspection pattern for app.py above)."""
    path = (
        PROJECT_ROOT
        / "templates"
        / "partials"
        / "index"
        / "body_preview_js.html"
    )
    return path.read_text(encoding="utf-8")


class TestPreviewJsRefetchSignature:
    """Finding 1 (major, confirmed 3-0): fetchEstimate()'s debounced
    refetch gate -- the `sig` JSON.stringify(...) that decides whether a
    NEW /api/estimate request is even scheduled -- used to only include
    budget/locations/roles. Switching industry (which materially changes
    classify_industry()'s channel mix and therefore the engine result) or
    client_name with an otherwise-unchanged brief matched the stale `sig`
    and silently kept showing the PREVIOUS industry's estimate. The gate
    must include every field estimatePayload() actually sends."""

    def test_fetch_estimate_signature_includes_industry_and_client_name(
        self, preview_js_source: str
    ) -> None:
        idx = preview_js_source.index("function fetchEstimate(m)")
        end_idx = preview_js_source.index("function channelOn(", idx)
        snippet = preview_js_source[idx:end_idx]
        sig_start = snippet.index("var sig = JSON.stringify(")
        sig_end = snippet.index(");", sig_start)
        sig_expr = snippet[sig_start:sig_end]
        assert "industry" in sig_expr, (
            "fetchEstimate()'s sig must include industry -- omitting it "
            "reproduces Finding 1 (stale preview on industry change)"
        )
        assert "clientName" in sig_expr, (
            "fetchEstimate()'s sig must include clientName -- omitting it "
            "reproduces Finding 1 (stale preview on client-name change)"
        )

    def test_signature_industry_and_client_name_match_payload_variables(
        self, preview_js_source: str
    ) -> None:
        """Not just present in the sig -- the SAME resolved values that
        estimatePayload() sends, so the gate can't drift from the actual
        request body again. Both fetchEstimate()'s sig and its call to
        estimatePayload() must reference the same local `industry` /
        `clientName` variables (resolved once via resolveIndustry() /
        val("clientName"))."""
        idx = preview_js_source.index("function fetchEstimate(m)")
        end_idx = preview_js_source.index("function channelOn(", idx)
        snippet = preview_js_source[idx:end_idx]
        assert "var industry = resolveIndustry();" in snippet
        assert 'var clientName = val("clientName");' in snippet
        assert "estimatePayload(m, roles, industry, clientName)" in snippet


# ---------------------------------------------------------------------------
# 4. Live E2E -- skipped when no server is reachable (matches
#    tests/test_e2e.py's convention exactly).
# ---------------------------------------------------------------------------

SERVER_HOST: str = os.environ.get("TEST_HOST", "localhost")
SERVER_PORT: int = int(os.environ.get("TEST_PORT", os.environ.get("PORT", "8000")))


def _server_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


_SERVER_UP: bool = _server_reachable(SERVER_HOST, SERVER_PORT)


def _post(
    path: str,
    body: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
) -> http.client.HTTPResponse:
    conn = http.client.HTTPConnection(SERVER_HOST, SERVER_PORT, timeout=10)
    hdrs = dict(headers or {})
    if body and "Content-Type" not in hdrs:
        hdrs["Content-Type"] = "application/json"
    conn.request("POST", path, body=body, headers=hdrs)
    return conn.getresponse()


@pytest.mark.skipif(
    not _SERVER_UP,
    reason=(
        f"Nova AI Suite server not reachable at {SERVER_HOST}:{SERVER_PORT}. "
        f"Start the server (or set TEST_HOST/TEST_PORT) to run E2E tests."
    ),
)
class TestEstimateEndpointLive:
    """Real HTTP requests against a running server."""

    def test_no_auth_returns_401(self) -> None:
        body = json.dumps({"budget": "$50,000", "roles": ["Driver"]}).encode("utf-8")
        resp = _post("/api/estimate", body=body)
        assert resp.status == 401
        resp.read()

    def test_malformed_json_returns_4xx(self) -> None:
        resp = _post(
            "/api/estimate",
            body=b"{not valid json",
            headers={"Origin": f"http://{SERVER_HOST}:{SERVER_PORT}"},
        )
        assert 400 <= resp.status < 500
        resp.read()

    def test_empty_body_returns_4xx(self) -> None:
        resp = _post(
            "/api/estimate",
            body=b"",
            headers={"Origin": f"http://{SERVER_HOST}:{SERVER_PORT}"},
        )
        assert 400 <= resp.status < 500
        resp.read()

    def test_manpower_brief_returns_prod_verified_numbers(self) -> None:
        body = json.dumps(MANPOWER_BRIEF).encode("utf-8")
        resp = _post(
            "/api/estimate",
            body=body,
            headers={"Origin": f"http://{SERVER_HOST}:{SERVER_PORT}"},
        )
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["est_hires"] == 48
        assert data["est_cph"] == pytest.approx(3125.0, abs=0.01)
        # NEVER the naive JS estimator's ballpark (~617 hires) -- this is
        # the exact regression the fix guards against.
        assert data["est_hires"] < 100


# ---------------------------------------------------------------------------
# 5. Same-origin gate hardening (2026-07-15 adversarial review).
#    Unit tests drive MediaPlanHandler._check_same_origin_auth directly
#    (it only touches self.headers, so a SimpleNamespace stands in for a
#    live handler); live tests run real HTTP against an in-process
#    app.ThreadedHTTPServer on an ephemeral port, matching
#    tests/test_generate_concurrency.py's live_server fixture pattern --
#    these ALWAYS run, unlike the TEST_PORT-gated class above.
# ---------------------------------------------------------------------------


def _gate_verdict(headers: dict[str, str]) -> bool:
    """Run _check_same_origin_auth against a fake handler with the given
    headers (dict.get matches http.client.HTTPMessage.get for our use)."""
    fake_handler = types.SimpleNamespace(headers=headers)
    return app.MediaPlanHandler._check_same_origin_auth(fake_handler)


class TestSameOriginGateUnit:
    """Parsed-host equality, never substring containment."""

    def test_referer_query_string_trick_rejected(self) -> None:
        """The verified bypass: 'localhost' as a query-string VALUE. The
        old substring gate passed this; the parsed hostname is evil.com."""
        assert _gate_verdict({"Referer": "https://evil.com/?x=localhost"}) is False

    def test_host_suffix_trick_rejected(self) -> None:
        """localhost as a subdomain label of an attacker's domain."""
        assert _gate_verdict({"Origin": "https://localhost.evil.com"}) is False

    def test_allowed_host_suffix_trick_rejected(self) -> None:
        """Same trick with the prod hostname as the leading labels."""
        assert (
            _gate_verdict(
                {"Origin": "https://media-plan-generator.onrender.com.evil.io"}
            )
            is False
        )

    def test_localhost_any_port_passes(self) -> None:
        """urlparse().hostname strips the port, so local dev keeps
        working on every port -- identical to the old gate's behavior
        for legit local origins."""
        for origin in ("http://localhost", "http://localhost:5001", "http://localhost:59999"):
            assert _gate_verdict({"Origin": origin}) is True, (
                f"legit local origin {origin!r} must pass the gate"
            )

    def test_prod_and_nova_origins_pass(self) -> None:
        assert _gate_verdict({"Origin": "https://nova.joveo.com"}) is True
        assert (
            _gate_verdict({"Origin": "https://media-plan-generator.onrender.com"})
            is True
        )

    def test_referer_with_path_passes_when_origin_absent(self) -> None:
        """Referer carries a full URL (path included) -- hostname parsing
        must still recognize it. Also covers the Origin-absent branch of
        the 'Origin if present, else Referer' precedence."""
        assert (
            _gate_verdict(
                {"Referer": "https://media-plan-generator.onrender.com/media-plan"}
            )
            is True
        )

    def test_origin_takes_precedence_over_referer(self) -> None:
        """Precedence preserved from the old gate: a present (evil)
        Origin is what gets checked, even if Referer looks legit."""
        assert (
            _gate_verdict(
                {"Origin": "https://evil.com", "Referer": "http://localhost:8000/"}
            )
            is False
        )

    def test_absent_null_and_unparseable_fail_closed(self) -> None:
        assert _gate_verdict({}) is False
        assert _gate_verdict({"Origin": "null"}) is False  # sandboxed iframe
        assert _gate_verdict({"Origin": "http://[::1"}) is False  # ValueError path


@pytest.fixture(scope="module")
def gate_server() -> Iterator[int]:
    """Real app.ThreadedHTTPServer on an ephemeral port (in-process
    daemon thread) -- the same fixture pattern as
    tests/test_generate_concurrency.py's live_server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="test-origin-gate-server"
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
        pytest.fail("gate_server did not start accepting connections in time")
    yield port
    server.shutdown()
    server.server_close()


def _post_to(
    port: int,
    path: str,
    body: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
) -> http.client.HTTPResponse:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    hdrs = dict(headers or {})
    if body and "Content-Type" not in hdrs:
        hdrs["Content-Type"] = "application/json"
    conn.request("POST", path, body=body, headers=hdrs)
    return conn.getresponse()


_VALID_ESTIMATE_BODY: bytes = json.dumps(
    {"budget": "$50,000", "roles": ["Warehouse Associate"]}
).encode("utf-8")


class TestOriginGateLive:
    """End-to-end over real HTTP: the bypass vectors get 401 on BOTH
    gated endpoints; legit origins still get through (NOT 401); and the
    Fix-1 crash class surfaces as 400, never 500, once past the gate."""

    def test_estimate_evil_referer_query_trick_401(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/estimate",
            body=_VALID_ESTIMATE_BODY,
            headers={"Referer": "https://evil.com/?x=localhost"},
        )
        assert resp.status == 401
        resp.read()

    def test_generate_evil_referer_query_trick_401(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/generate",
            body=b'{"probe": "referer-trick"}',
            headers={"Referer": "https://evil.com/?x=localhost"},
        )
        assert resp.status == 401
        resp.read()

    def test_estimate_host_suffix_trick_401(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/estimate",
            body=_VALID_ESTIMATE_BODY,
            headers={"Origin": "https://localhost.evil.com"},
        )
        assert resp.status == 401
        resp.read()

    def test_generate_host_suffix_trick_401(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/generate",
            body=b'{"probe": "host-suffix-trick"}',
            headers={"Origin": "https://localhost.evil.com"},
        )
        assert resp.status == 401
        resp.read()

    def test_estimate_localhost_origin_passes_gate(self, gate_server: int) -> None:
        """Legit local origin (with port) computes a real estimate --
        the hardened gate must not break the wizard's own preview."""
        resp = _post_to(
            gate_server,
            "/api/estimate",
            body=_VALID_ESTIMATE_BODY,
            headers={"Origin": f"http://localhost:{gate_server}"},
        )
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["est_hires"] >= 0

    def test_estimate_nova_origin_passes_gate(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/estimate",
            body=_VALID_ESTIMATE_BODY,
            headers={"Origin": "https://nova.joveo.com"},
        )
        assert resp.status != 401
        assert resp.status == 200
        resp.read()

    def test_generate_localhost_origin_passes_gate(self, gate_server: int) -> None:
        """Gate-pass is what's under test, not generation: an invalid
        JSON body makes /api/generate return a fast 400 right after the
        auth gate (no 60-120s pipeline). Anything but 401 proves the
        parsed-host gate admitted the legit origin."""
        resp = _post_to(
            gate_server,
            "/api/generate",
            body=b"{not valid json: gate-pass probe",
            headers={"Origin": f"http://localhost:{gate_server}"},
        )
        assert resp.status != 401
        assert resp.status == 400
        resp.read()

    def test_generate_nova_origin_passes_gate(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/generate",
            body=b"",  # empty body -> fast 400 after the gate
            headers={"Origin": "https://nova.joveo.com"},
        )
        assert resp.status != 401
        assert resp.status == 400
        resp.read()

    # -- Fix-1 crash class, end-to-end: 4xx (VALIDATION_ERROR), never 500 --

    def test_estimate_exact_repro_body_is_400_not_500(self, gate_server: int) -> None:
        """The EXACT verified 2026-07-15 repro body that used to 500."""
        resp = _post_to(
            gate_server,
            "/api/estimate",
            body=json.dumps(
                {
                    "budget": "$150,000",
                    "roles": [{"title": "X", "count": "abc"}],
                    "locations": ["Dallas, TX"],
                }
            ).encode("utf-8"),
            headers={"Origin": f"http://localhost:{gate_server}"},
        )
        assert resp.status == 400, (
            f"expected 400 for count='abc', got {resp.status} -- a 500 here "
            f"means the unguarded int() cast regressed"
        )
        resp.read()

    def test_estimate_list_count_is_400_not_500(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/estimate",
            body=json.dumps(
                {
                    "budget": "$150,000",
                    "roles": [{"title": "X", "count": [1, 2]}],
                    "locations": ["Dallas, TX"],
                }
            ).encode("utf-8"),
            headers={"Origin": f"http://localhost:{gate_server}"},
        )
        assert resp.status == 400
        resp.read()

    def test_estimate_month_name_is_400_not_500(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/estimate",
            body=json.dumps(
                {
                    "budget": "$150,000",
                    "roles": [{"title": "X", "count": 1}],
                    "locations": ["Dallas, TX"],
                    "campaign_start_month": "March",
                }
            ).encode("utf-8"),
            headers={"Origin": f"http://localhost:{gate_server}"},
        )
        assert resp.status == 400
        resp.read()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
