"""Delta verifier on mpg-input-parity @5a59c1b (2026-10-01), items 4a and
4c -- the /api/generate and /api/estimate request boundary.

4a. A JSON NUMBER budget 750.125 read $750.13 on /api/estimate (which gets
    the number) but $750,125 on /api/generate: the generate sanitizer
    str()'d every number and the TEXT reader then took "750.125" for
    750,125 (one "." before exactly three digits is a thousands separator
    in typed text). Numeric budgets now stay numbers through the boundary.
4c. A client-sent "_budget_normalized": true skipped the canonical budget
    rewrite on /api/generate ("1.5 million" validated, then every
    downstream reader re-parsed the raw text) and made /api/estimate treat
    a per-month budget as a campaign total. Every "_"-prefixed key is now
    dropped at both boundaries -- those names belong to the server.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import app
from shared_utils import parse_budget
from tests.live_server import live_port, post_json  # noqa: F401 (fixture)


def _generate_view(payload: dict) -> dict:
    """The request as /api/generate's pipeline sees it after the boundary
    and the budget block (deck, workbook and gate all read THIS dict)."""
    data = app._sanitize_generate_request(dict(payload))
    assert app._resolve_request_budget(data).ok
    app._normalize_request_budget(data, log=False)
    return data


@pytest.mark.parametrize("number", [750.125, 999.995, 101.5, 123.456, 1234.567])
def test_json_number_budgets_read_the_same_on_both_endpoints(live_port, number):  # noqa: F811
    status, est = post_json(live_port, "/api/estimate", {"budget": number, "budget_only": True})
    assert status == 200, est
    data = _generate_view({"budget": number})
    assert est["budget"]["total"] == data["_budget_resolution"]["total"]
    assert parse_budget(data["budget"]) == pytest.approx(est["budget"]["total"], abs=0.005)
    assert abs(est["budget"]["total"] - number) < 0.01  # never x1000


def test_text_budgets_keep_the_text_reading():
    # typed text is NOT a JSON number: "750.125" is 750,125 (thousands ".")
    assert _generate_view({"budget": "750.125"})["_budget_resolution"]["total"] == 750_125


def test_client_cannot_skip_the_budget_rewrite():
    data = _generate_view({"budget_range": "1.5 million", "_budget_normalized": True,
                           "_budget_resolution": {"ok": True, "total": 1.5}})
    assert data["budget"] == "1,500,000"
    assert parse_budget(data["budget"]) == 1_500_000  # every downstream reader
    assert data["_budget_resolution"]["total"] == 1_500_000


@pytest.mark.parametrize(
    "key",
    ["_budget_normalized", "_budget_resolution", "_budget_multiplier", "_role_tiers",
     "_collar_type", "_location_resolution", "_industry_legacy_key", "_synthesized", "_plan_id"],
)
def test_internal_keys_never_cross_either_boundary(key):
    assert key not in app._sanitize_generate_request({key: True, "budget": "50000"})
    est = app._compute_plan_estimate({key: {"x": 1}, "budget_range": "50000", "budget_only": True})
    assert est["budget"]["total"] == 50000


def test_estimate_ignores_a_client_normalized_flag(live_port):  # noqa: F811
    status, est = post_json(
        live_port, "/api/estimate",
        {"budget_range": "1.5 million", "_budget_normalized": True, "budget_period": "monthly",
         "campaign_duration": "6 months", "budget_only": True},
    )
    assert status == 200, est
    assert est["budget"]["total"] == 9_000_000  # scaled once, from the text


def test_boundary_still_strips_markup_and_keeps_booleans():
    out = app._sanitize_generate_request(
        {"client_name": "<b>Acme</b>", "include_x": False, "budget": 5e4, "hires": 12}
    )
    assert out == {"client_name": "Acme", "include_x": False, "budget": 50000.0, "hires": "12"}


def test_generate_handler_uses_the_one_boundary():
    src = Path(app.__file__).read_text(encoding="utf-8")
    assert src.count("data = _sanitize_generate_request(data)") == 1
    assert "data[_skey] = _sanitize_request_value(data[_skey])" not in src
    est = src[src.index("def _compute_plan_estimate(") :]
    assert "_drop_internal_request_keys(dict(brief))" in est[: est.index("\ndef ")]
