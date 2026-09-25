"""Regression tests: several sites entered at once must reach the plan as
several locations, never collapse into one.

Client report 2026-09-24 (Hershey Company plan, Brendan Macomber): ten
sites entered, the plan covered four. Prod logs for that run show the
location list was already four entries long when data synthesis started
("Starting data synthesis (... locations=4 ...)"), and a local run of the
same /api/generate code with ten clean "City, ST" entries produced a
workbook with all ten (Market Intelligence, Sources & Confidence and
Quality Intelligence sheets all list 10/10). So the server does not drop
list entries; the list arrived short.

The wizard's location tag input (templates/partials/index/body_app_js.html)
was the only place that can shrink it. It commits a tag on the "," KEY,
so it only works when sites are typed one keystroke at a time. Any text
that reaches the box without per-character keydowns (a paste, autofill,
or a list copied from a document) was committed on Enter/blur as ONE tag
via ``this.value.replace(",", "")``, which strips only the first comma:
ten pasted "City, ST" sites became the single tag "Hershey PA, Hazleton,
PA, Lancaster, PA, ..." -- one location. Full state names broke the typed
path too: "Hazleton, Pennsylvania" committed "Hazleton", then the second
"Pennsylvania" was rejected as a duplicate tag WITHOUT clearing the box, so
the next site was glued onto it ("PennsylvaniaLancaster"). The server's
string branch had the same defect in another form: it split on every
comma, turning "Hershey, PA, Hazleton, PA" into four halves.

These tests run the REAL shipped handler code (sliced out of the template
and executed in node against a minimal DOM stand-in), plus the server's
request-boundary normalizer, so they fail on the pre-fix code.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_JS_PATH = _ROOT / "templates" / "partials" / "index" / "body_app_js.html"

HERSHEY_SITES = [
    "Hershey, PA",
    "Hazleton, PA",
    "Lancaster, PA",
    "Memphis, TN",
    "Robinson, IL",
    "Stuarts Draft, VA",
    "Ogden, UT",
    "Reading, PA",
    "Lititz, PA",
    "Palmyra, PA",
]
HERSHEY_SITES_FULL = [
    "Hershey, Pennsylvania",
    "Hazleton, Pennsylvania",
    "Lancaster, Pennsylvania",
    "Memphis, Tennessee",
    "Robinson, Illinois",
    "Stuarts Draft, Virginia",
    "Ogden, Utah",
    "Reading, Pennsylvania",
    "Lititz, Pennsylvania",
    "Palmyra, Pennsylvania",
]


# ── Server: request-boundary normalization ──────────────────────────────


def _normalize(value):
    import app

    data = {"locations": value}
    app._normalize_location_field(data)
    return data["locations"]


def test_server_splits_pasted_city_state_list_in_one_entry():
    assert _normalize([", ".join(HERSHEY_SITES)]) == HERSHEY_SITES


def test_server_splits_newline_and_semicolon_lists():
    assert _normalize(["\n".join(HERSHEY_SITES)]) == HERSHEY_SITES
    assert _normalize(["; ".join(HERSHEY_SITES_FULL)]) == HERSHEY_SITES_FULL


def test_server_string_payload_keeps_city_state_pairs():
    # Pre-fix: split on every comma -> ["Hershey", "PA", "Hazleton", "PA", ...]
    assert _normalize(", ".join(HERSHEY_SITES)) == HERSHEY_SITES
    # Plain comma list of cities keeps the legacy one-per-comma behaviour.
    assert _normalize("Atlanta, Chicago, Miami") == ["Atlanta", "Chicago", "Miami"]


def test_server_mixed_list_is_one_entry_per_site():
    raw = ["Hershey, PA", "Hazleton, Pennsylvania", "Lancaster PA, Memphis TN, Robinson, IL"]
    assert _normalize(raw) == [
        "Hershey, PA",
        "Hazleton, Pennsylvania",
        "Lancaster PA",
        "Memphis TN",
        "Robinson, IL",
    ]


@pytest.mark.parametrize(
    "entry",
    [
        "Springfield, MO",
        "Cook County, IL",
        "Hershey, PA, USA",
        "Washington, D.C.",
        "Paris, France",
        "Atlanta, Chicago",  # a single list entry with no state pairs stays as typed
        "Remote, TX",
        "New York, New York",
    ],
)
def test_server_leaves_single_site_entries_untouched(entry):
    assert _normalize([entry]) == [entry]


def test_server_never_drops_or_dedupes_entries():
    raw = HERSHEY_SITES + ["Hershey, PA"]
    assert _normalize(raw) == raw


# ── Wizard: the real tag-input handlers, executed in node ───────────────

_HARNESS = r"""
const [SRC_PATH, ACTIONS_JSON] = process.argv.slice(-2);
const SRC = require("fs").readFileSync(SRC_PATH, "utf8");
const ACTIONS = JSON.parse(ACTIONS_JSON);

function slice(startMarker, endMarker) {
  const s = SRC.indexOf(startMarker);
  const e = SRC.indexOf(endMarker, s);
  if (s < 0 || e < 0) throw new Error("marker not found: " + startMarker);
  return SRC.slice(s, e);
}
const statesSrc = SRC.match(/const VALID_US_STATES = new Set\(\[[\s\S]*?\]\);/)[0];
const countriesSrc = SRC.match(/const VALID_COUNTRIES = new Set\(\[[\s\S]*?\]\);/)[0];
const handlersSrc = slice("// Tags input for locations", "function removeTag(");

function makeEl(id) {
  return {
    id, value: "", listeners: {},
    addEventListener(t, f) { (this.listeners[t] = this.listeners[t] || []).push(f); },
    fire(t, ev) { (this.listeners[t] || []).forEach((f) => f.call(this, ev)); },
  };
}
const els = {};
const document = { getElementById: (id) => (els[id] = els[id] || makeEl(id)) };
let locations = [], roles = [], competitors = [];
// Chromium fires blur on a focused input when renderTags detaches it
// (container.innerHTML = "") -- observed live in the wizard; mirror it.
function renderTags(containerId, inputId) { document.getElementById(inputId).fire("blur", {}); }
function updateRegionalSections() {}
function scheduleLocationResolution() {}

eval(statesSrc + "\n" + countriesSrc + "\n" + handlersSrc);

const input = document.getElementById("locationInput");
function key(k) {
  const ev = { key: k, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; } };
  input.fire("keydown", ev);
  return ev.defaultPrevented;
}
for (const a of ACTIONS) {
  if (a.type === "type") {
    for (const ch of a.text) { if (!key(ch)) input.value += ch; }
  } else if (a.type === "key") {
    key(a.key);
  } else if (a.type === "paste") {
    const ev = {
      defaultPrevented: false, preventDefault() { this.defaultPrevented = true; },
      clipboardData: { getData: () => a.text },
    };
    input.fire("paste", ev);
    // <input type=text> value sanitization strips line breaks on insert.
    if (!ev.defaultPrevented) input.value += a.text.replace(/[\r\n]/g, "");
  } else if (a.type === "blur") {
    input.fire("blur", {});
  }
}
process.stdout.write(JSON.stringify({ locations, input: input.value }));
"""


def _run_wizard(actions: list[dict]) -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node not available")
    proc = subprocess.run(
        [node, "-e", _HARNESS, "--", str(_JS_PATH), json.dumps(actions)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def _type_each(sites: list[str], commit_key: str = "Enter") -> list[dict]:
    actions: list[dict] = []
    for site in sites:
        actions.append({"type": "type", "text": site})
        actions.append({"type": "key", "key": commit_key})
    return actions


def test_wizard_paste_of_ten_sites_yields_ten_tags():
    for sep in (", ", "\n", "; "):
        out = _run_wizard(
            [{"type": "paste", "text": sep.join(HERSHEY_SITES)}, {"type": "key", "key": "Enter"}]
        )
        assert out["locations"] == HERSHEY_SITES, (sep, out)


def test_wizard_pasted_list_committed_by_blur_yields_ten_tags():
    out = _run_wizard(
        [{"type": "paste", "text": ", ".join(HERSHEY_SITES)}, {"type": "blur"}]
    )
    assert out["locations"] == HERSHEY_SITES


def test_wizard_typed_city_state_code_one_per_enter():
    out = _run_wizard(_type_each(HERSHEY_SITES))
    assert out["locations"] == HERSHEY_SITES


def test_wizard_typed_full_state_names_one_per_enter():
    out = _run_wizard(_type_each(HERSHEY_SITES_FULL))
    assert out["locations"] == HERSHEY_SITES_FULL
    assert out["input"] == ""


def test_wizard_typed_run_on_list_with_commas():
    # One long typed line, commas between everything (comma key commits).
    out = _run_wizard(
        [{"type": "type", "text": ", ".join(HERSHEY_SITES)}, {"type": "key", "key": "Enter"}]
    )
    assert out["locations"] == HERSHEY_SITES


def test_wizard_mixed_codes_and_names():
    mixed = [
        "Hershey, PA",
        "Hazleton, Pennsylvania",
        "Lancaster, PA",
        "Memphis, Tennessee",
        "Robinson, IL",
    ]
    out = _run_wizard(_type_each(mixed))
    assert out["locations"] == mixed


def test_wizard_existing_behaviour_preserved():
    # Rapid-add bare cities by comma.
    out = _run_wizard([{"type": "type", "text": "Atlanta, Chicago, Miami"}, {"type": "key", "key": "Enter"}])
    assert out["locations"] == ["Atlanta", "Chicago", "Miami"]
    # Two independent bare states stay two tags.
    out = _run_wizard([{"type": "type", "text": "CA, NY"}, {"type": "key", "key": "Enter"}])
    assert out["locations"] == ["CA", "NY"]
    # A location KIND never absorbs a state.
    out = _run_wizard([{"type": "type", "text": "Remote, TX"}, {"type": "key", "key": "Enter"}])
    assert out["locations"] == ["Remote", "TX"]
    # A bare full state name on its own is a statewide location.
    out = _run_wizard(_type_each(["Pennsylvania", "Ohio"]))
    assert out["locations"] == ["Pennsylvania", "Ohio"]


def test_wizard_parser_block_is_self_contained():
    src = _JS_PATH.read_text(encoding="utf-8")
    assert re.search(r"function parseLocationEntries\(", src)
