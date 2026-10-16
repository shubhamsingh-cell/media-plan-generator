"""The "Exclude job boards" field, request boundary to wizard.

Pre-fix no surface could exclude a publisher: the wizard had only
channel-category checkboxes, /api/generate carried no field and nothing
validated one.  Now the wizard's tag input feeds ``excluded_publishers`` into
the ONE shared payload builder (generate and the live preview send the same
list), the server validates it identically on /api/generate and /api/estimate
(400 naming the field) and rewrites it to a clean unique list.
"""

from __future__ import annotations

import pytest

import app
import publisher_filter
import wizard_inputs
from tests.live_server import live_port, post_json  # noqa: F401 (fixture)
from tests.test_wizard_plan_inputs_parity import _LOCS, _ROLES, _STATE
from tests.wizard_js_harness import APP_JS, NODE, wizard_payloads

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")
LIMITS = wizard_inputs.INPUT_LIMITS


def test_limits_mirror_the_matcher_caps():
    assert LIMITS["excluded_publishers_max"] == publisher_filter.MAX_EXCLUDED
    assert LIMITS["excluded_publisher_chars"] == publisher_filter.MAX_NAME_CHARS


@pytest.mark.parametrize(
    "raw,ok",
    [
        (None, True),
        ([], True),
        (["HelloWork", "indeed.com"], True),
        ("Indeed, Seek", True),
        ([f"Board {i}" for i in range(LIMITS["excluded_publishers_max"] + 1)], False),
        (["x" * (LIMITS["excluded_publisher_chars"] + 1)], False),
        ([1, 2], False),
        ({"a": 1}, False),
    ],
)
def test_problem_rule(raw, ok):
    code, message = app._excluded_publishers_problem(raw)
    assert (code == "") is ok, (raw, code, message)
    assert bool(message) is (not ok)


def test_normalize_rewrites_to_a_clean_unique_list():
    data = {"excluded_publishers": [" indeed.com ", "Indeed", "<i>HelloWork</i>", ""]}
    app._normalize_excluded_publishers(data)
    assert data["excluded_publishers"] == ["indeed.com", "HelloWork"]
    absent: dict = {}
    app._normalize_excluded_publishers(absent)
    assert "excluded_publishers" not in absent


def _generate_payload(**over) -> dict:
    base = {
        "requester_name": "QA Bot",
        "requester_email": "qa@joveo.com",
        "client_name": "Exclusion Co",
        "use_case": "Hiring welders",
        "target_roles": ["Welder"],
        "locations": ["Houston, TX"],
        "budget_range": "abc",  # anything that passes validation stops at the budget
        "campaign_duration": "3 months",
    }
    base.update(over)
    return base


@pytest.mark.parametrize(
    "bad",
    [
        [f"Board {i}" for i in range(LIMITS["excluded_publishers_max"] + 1)],
        ["x" * (LIMITS["excluded_publisher_chars"] + 1)],
    ],
)
def test_generate_400_names_the_field(live_port, bad):  # noqa: F811
    status, body = post_json(
        live_port, "/api/generate", _generate_payload(excluded_publishers=bad)
    )
    assert status == 400 and body.get("field") == "excluded_publishers", (status, body)


def test_generate_accepts_a_valid_list_past_the_exclusion_check(
    live_port,
):  # noqa: F811
    status, body = post_json(
        live_port,
        "/api/generate",
        _generate_payload(excluded_publishers=["HelloWork", "indeed.com"]),
    )
    # stops at the (deliberately invalid) budget, not at the exclusion field
    assert status == 400 and body.get("field") != "excluded_publishers", body


def test_estimate_validates_the_same_rule(live_port):  # noqa: F811
    brief = {
        "budget_range": "250000",
        "industry": "blue_collar_trades",
        "client_name": "Exclusion Co",
        "target_roles": ["Welder"],
        "locations": ["Houston, TX"],
    }
    status, body = post_json(
        live_port, "/api/estimate", dict(brief, excluded_publishers=["x" * 200])
    )
    assert status == 400 and body.get("field") == "excluded_publishers", body
    status, body = post_json(
        live_port, "/api/estimate", dict(brief, excluded_publishers=["indeed.com"])
    )
    assert status == 200, body


@needs_node
def test_generate_and_preview_send_the_same_exclusion_list():
    got = wizard_payloads(
        _STATE,
        selected_industry="blue_collar_trades",
        locations=_LOCS,
        roles=_ROLES,
        excluded_publishers=["indeed.com", "HelloWork"],
    )
    assert got["generate"]["excluded_publishers"] == ["indeed.com", "HelloWork"]
    assert got["estimate"]["excluded_publishers"] == ["indeed.com", "HelloWork"]


def test_wizard_has_the_tag_input_review_row_and_draft_restore():
    src = APP_JS.read_text(encoding="utf-8")
    html = (APP_JS.parent / "body_content.html").read_text(encoding="utf-8")
    assert 'id="excludedPublisherInput"' in html
    assert 'id="excludedPublishersContainer"' in html
    assert 'id="reviewExcludedPublishers"' in html
    for needle in (
        "excluded_publishers: excludedPublishers.slice()",  # shared payload builder
        "excludedPublishers: excludedPublishers || []",  # saveFormState
        "s.excludedPublishers",  # restore
        "excludedPublishersContainer: [",  # tag-count limit
        'excluded_publishers: "excludedPublisherInput"',  # server error -> field
    ):
        assert needle in src, needle
    # Enter, comma and blur all commit
    assert 'e.key === "Enter" || e.key === ","' in src
    assert 'exInput.addEventListener("blur"' in src
