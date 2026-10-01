"""Every /api/generate validation 400 names its field, and client names are
judged by one rule on both endpoints (independent verifier on 2ad8904,
2026-10-01).

Pre-fix: seven generate 400s carried no ``field`` (roles > 50, required
fields, two location checks, two hire-volume checks, start month), so the
wizard could not take the user to the problem; and the client-name rule
``re.search(r"[a-zA-Z]{2,}")`` rejected "3M", "H&M", "P&G", "A&W", "O2",
"X" and every CJK / Cyrillic / Arabic name at Generate while /api/estimate
accepted them. Now: one letter or digit in any script
(app._client_name_problem), the same rule in the wizard's JS.
"""

from __future__ import annotations

import json

import pytest

import app
from tests.live_server import live_port, post_json  # noqa: F401 (fixture)
from tests.wizard_js_harness import APP_JS, NODE, js_block, run_node

ACCEPTED = ["3M", "H&M", "P&G", "A&W", "O2", "X", "朝日新聞", "Яндекс", "شركة الاتصالات", "Société Générale", "  Acme  "]
REJECTED = ["!!!", "&&", "😀🚀", "—", "..."]


def _payload(tag: str, **over) -> dict:
    base = {
        "requester_name": "QA Bot",
        "requester_email": "qa@joveo.com",
        "client_name": f"Field Co {tag}",
        "use_case": "Hiring welders",
        "target_roles": ["Welder"],
        "locations": ["Houston, TX"],
        "budget_range": "50000",
        "campaign_duration": "3 months",
    }
    base.update(over)
    return base


@pytest.mark.parametrize(
    "tag,over,field",
    [
        ("roles50", {"target_roles": [f"Role {i}" for i in range(51)]}, "target_roles"),
        ("noreq", {"requester_name": ""}, "requester_name"),
        ("bademail", {"requester_email": "not-an-email"}, "requester_email"),
        ("noroles", {"target_roles": []}, "target_roles"),
        ("locfake", {"locations": ["test"]}, "locations"),
        ("locnum", {"locations": ["12"]}, "locations"),
        ("hire0", {"hire_volume": "0 hires"}, "hire_volume"),
        ("hirebig", {"hire_volume": "200000 hires"}, "hire_volume"),
        ("month13", {"campaign_start_month": 13}, "campaign_start_month"),
        ("badname", {"client_name": "!!!"}, "client_name"),
    ],
)
def test_every_generate_validation_400_names_its_field(live_port, tag, over, field):  # noqa: F811
    status, body = post_json(live_port, "/api/generate", _payload(tag, **over))
    assert status == 400, (status, body)
    assert body.get("field") == field, body


@pytest.mark.parametrize("name", ACCEPTED)
def test_generate_accepts_real_client_names(live_port, name):  # noqa: F811
    # campaign_start_month 13 fails AFTER the name check: reaching it proves
    # the name passed, without running a whole generation.
    status, body = post_json(
        live_port, "/api/generate", _payload(name, client_name=name, campaign_start_month=13)
    )
    assert status == 400 and body.get("field") == "campaign_start_month", (name, body)


@pytest.mark.parametrize("name", REJECTED)
def test_generate_rejects_names_without_a_letter_or_digit(live_port, name):  # noqa: F811
    status, body = post_json(live_port, "/api/generate", _payload(name, client_name=name))
    assert status == 400 and body.get("field") == "client_name", (name, body)
    assert "needs a letter or digit" in body["error"]


def test_blank_client_name_is_still_required(live_port):  # noqa: F811
    status, body = post_json(live_port, "/api/generate", _payload("blank", client_name="   "))
    assert status == 400 and body.get("field") == "client_name"
    assert "Client name" in body["error"]


_EST = {
    "budget_range": "100000",
    "industry": "blue_collar_trades",
    "target_roles": ["Welder"],
    "locations": ["Houston, TX"],
}


@pytest.mark.parametrize("name", ACCEPTED + [""])
def test_estimate_uses_the_same_name_rule_accepted(name):
    assert app._compute_plan_estimate(dict(_EST, client_name=name))["est_hires"] >= 0


@pytest.mark.parametrize("name", REJECTED)
def test_estimate_uses_the_same_name_rule_rejected(name):
    with pytest.raises(app._EstimateValidationError) as exc:
        app._compute_plan_estimate(dict(_EST, client_name=name))
    assert exc.value.field == "client_name"


def test_name_length_limit_is_kept():
    code, message = app._client_name_problem("A" * 201)
    assert code == "too_long" and "max 200" in message
    assert app._client_name_problem("A" * 200) == ("", "")


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_wizard_applies_the_same_name_rule():
    src = APP_JS.read_text(encoding="utf-8")
    rx = src[src.index("var _novaNameCharRx = (function () {") :]
    rx = rx[: rx.index("})();") + len("})();")]
    script = "\n".join(
        [
            rx,
            js_block(src, "function novaClientNameProblem("),
            "var names = " + json.dumps(ACCEPTED + REJECTED) + ";",
            "process.stdout.write(JSON.stringify(names.map(function (n) {",
            "  return novaClientNameProblem(n) === ''; })));",
        ]
    )
    got = run_node(script)
    assert got == [True] * len(ACCEPTED) + [False] * len(REJECTED)
    for name, ok in zip(ACCEPTED + REJECTED, got):
        assert (app._client_name_problem(name)[0] == "") is ok, name
