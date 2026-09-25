"""Regressions from d78bba1 (one location per site), found by adversarial
review, in the wizard's parseLocationEntries/addTag and the server's
plan_location.split_location_entries:

1. A full state name that is also a city ("New York", "Washington",
   "Indiana") was always treated as a bare state, so "New York, NY",
   "Washington, DC" and "Indiana, PA" split into two tags.
2. Three-part international places ("Bengaluru, Karnataka, India",
   "Toronto, Ontario, Canada", "Sydney, NSW, Australia") split into two
   sites (or three, server-side).

Both paths run the REAL shipped code: the wizard handlers in node via the
harness in tests/test_location_multi_site_entry.py, the server normalizer
in-process.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import plan_location
from tests.test_location_multi_site_entry import _normalize, _run_wizard, _type_each

_JS_PATH = Path(__file__).resolve().parent.parent / "templates" / "partials" / "index" / "body_app_js.html"

STATE_NAME_CITIES = ["New York, NY", "Washington, DC", "Indiana, PA"]
INTL_THREE_PART = [
    "Bengaluru, Karnataka, India",
    "Toronto, Ontario, Canada",
    "Sydney, NSW, Australia",
]
SINGLE_SITES = STATE_NAME_CITIES + INTL_THREE_PART + ["Toronto, ON", "Mumbai, Maharashtra"]


# ── Wizard: pasted ──────────────────────────────────────────────────────


@pytest.mark.parametrize("site", SINGLE_SITES)
def test_wizard_paste_single_site_is_one_tag(site):
    out = _run_wizard([{"type": "paste", "text": site}, {"type": "key", "key": "Enter"}])
    assert out["locations"] == [site]


def test_wizard_paste_list_of_such_sites_is_one_tag_each():
    for sep in ("\n", "; "):
        out = _run_wizard(
            [{"type": "paste", "text": sep.join(SINGLE_SITES)}, {"type": "key", "key": "Enter"}]
        )
        assert out["locations"] == SINGLE_SITES, sep


def test_wizard_paste_comma_list_of_state_name_cities():
    out = _run_wizard(
        [{"type": "paste", "text": "New York, NY, Washington, DC"}, {"type": "blur"}]
    )
    assert out["locations"] == ["New York, NY", "Washington, DC"]


# ── Wizard: typed (the comma key commits each token) ────────────────────


@pytest.mark.parametrize("site", SINGLE_SITES)
def test_wizard_typed_single_site_is_one_tag(site):
    out = _run_wizard([{"type": "type", "text": site}, {"type": "key", "key": "Enter"}])
    assert out["locations"] == [site]
    assert out["input"] == ""


def test_wizard_typed_one_per_enter():
    out = _run_wizard(_type_each(SINGLE_SITES))
    assert out["locations"] == SINGLE_SITES


# ── Wizard: behaviour that must NOT change ──────────────────────────────


def test_wizard_bare_states_and_cities_still_split():
    cases = {
        "CA, NY": ["CA", "NY"],
        "Texas, California": ["Texas", "California"],
        "Remote, TX": ["Remote", "TX"],
        "Atlanta, Chicago, Miami": ["Atlanta", "Chicago", "Miami"],
        "London, Manchester, UK": ["London", "Manchester, UK"],
    }
    for text, expected in cases.items():
        typed = _run_wizard([{"type": "type", "text": text}, {"type": "key", "key": "Enter"}])
        pasted = _run_wizard([{"type": "paste", "text": text}, {"type": "key", "key": "Enter"}])
        assert typed["locations"] == expected, ("typed", text, typed)
        assert pasted["locations"] == expected, ("pasted", text, pasted)
    out = _run_wizard(_type_each(["New York", "Washington"]))
    assert out["locations"] == ["New York", "Washington"]


# ── Server ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("site", SINGLE_SITES)
def test_server_string_payload_keeps_single_site(site):
    assert _normalize(site) == [site]


def test_server_list_item_with_several_such_sites_splits_per_site():
    assert _normalize(["New York, NY, Washington, DC"]) == ["New York, NY", "Washington, DC"]
    assert _normalize(["Bengaluru, Karnataka, India, Toronto, ON, Canada"]) == [
        "Bengaluru, Karnataka, India",
        "Toronto, ON, Canada",
    ]


def test_server_bare_states_still_split():
    assert _normalize("CA, NY") == ["CA", "NY"]
    assert _normalize("Texas, California") == ["Texas", "California"]


# ── Parity: the wizard and the server use the same token lists ──────────


def _js_set(name: str) -> set:
    src = _JS_PATH.read_text(encoding="utf-8")
    body = re.search(rf"const {name} = new Set\(\[([\s\S]*?)\]\);", src).group(1)
    return {t.lower() for t in re.findall(r'"([^"]+)"', body)}


def test_region_lists_match():
    assert _js_set("INTL_REGIONS") == set(plan_location._INTL_REGION_TOKENS)


def test_country_lists_match():
    assert _js_set("VALID_COUNTRIES") == set(plan_location._SPLIT_COUNTRY_TOKENS) - {
        "united states of america",
        "america",
    }
