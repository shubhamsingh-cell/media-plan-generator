"""The hub's hero "Prediction Engine" card shows Nova's engine, not a tuned model.

Until 2026-10-17 the card (static/js/hub.js) computed every figure from
hand-tuned per-role constants -- a $18 cost per application for a
Registered Nurse against $40 in data/joveo_2026_benchmarks.json, $9 for a
CDL Driver against $34 -- with a hire yield 5-8x cheaper than the calibrated
engine (tests/test_funnel_calibration.py), under an "Illustrative model"
label; story 4 pinned "$1,064" to that model's default output.

Now every figure is ``budget_engine.calculate_budget_allocation`` through
``app._compute_plan_estimate`` -- the un-enriched call POST /api/estimate
makes -- and this file pins the places that must agree:

  1. the chips and slider in templates/hub.html ARE ``proof_claims.HUB_PE_*``
     (roles with their industries, markets, budget min/max/step), and the
     default chips are ``HUB_PE_DEFAULT_BRIEF``;
  2. the served hub renders the engine's answer for that brief into the card,
     into story 4's figure, and into the #pe-default blob hub.js seeds from;
  3. every other scenario comes from GET /api/hub/prediction-grid -- the same
     engine call priced once per process -- and hub.js looks it up there,
     carrying no constants and making no per-drag request.

Plus: the whole grid is sane (every scenario at least two hires, hires never
falling as the budget rises, every funded channel has a row), and the
line under the headline states the plan-efficiency case in the deck's terms.

Runs under pytest, or standalone:
``python3 -m pytest tests/test_hub_prediction_engine.py -q``.
"""

from __future__ import annotations

import gzip
import html as html_lib
import http.client
import itertools
import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402  (registers the estimator with proof_claims)
import proof_claims  # noqa: E402
from tests.live_server import live_port, post_json  # noqa: E402,F401 (fixture)

HUB = PROJECT_ROOT / "templates" / "hub.html"
HUB_JS = PROJECT_ROOT / "static" / "js" / "hub.js"


def _slider_budgets(card: str) -> tuple[int, ...]:
    """The slider's min / default / max -- the budgets a visitor can reach."""
    m = re.search(r"<input[^>]*data-pe-range[^>]*>", card, re.S)
    assert m, "budget range input not found"
    attrs = dict(re.findall(r'(min|max|value)="(\d+)"', m.group(0)))
    assert set(attrs) == {"min", "max", "value"}, attrs
    return tuple(sorted({int(attrs["min"]), int(attrs["value"]), int(attrs["max"])}))


def _card(src: str) -> str:
    m = re.search(
        r'<div\s+class="pe-card pe-card--hero"\s+id="plan-showcase"[^>]*>.*?</div>\s*<!-- The default scenario',
        src,
        re.S,
    )
    assert m, "hero card markup not found"
    return m.group(0)


def _chips(card: str, kind: str) -> list[dict[str, str]]:
    """Each chip's attributes, in markup order. ``kind`` is role / loc."""
    out = []
    for m in re.finditer(rf'<button[^>]*data-{kind}="\d+"[^>]*>', card):
        attrs = {
            k: html_lib.unescape(v)
            for k, v in re.findall(r'([\w-]+)="([^"]*)"', m.group(0))
        }
        out.append(attrs)
    assert out, f"no {kind} chips"
    return out


def _markup_brief(
    role: dict[str, str], loc: dict[str, str], budget: int
) -> dict[str, Any]:
    return {
        "budget": f"${budget:,}",
        "industry": role["data-pe-industry"],
        "roles": [role["data-pe-title"]],
        "locations": [loc["data-pe-location"]],
    }


def _default_brief_from_markup() -> dict[str, Any]:
    card = _card(HUB.read_text(encoding="utf-8"))
    role = next(c for c in _chips(card, "role") if "is-on" in c["class"])
    loc = next(c for c in _chips(card, "loc") if "is-on" in c["class"])
    range_input = re.search(r"<input[^>]*data-pe-range[^>]*>", card, re.S)
    assert range_input, "budget range input not found"
    value = re.search(r'value="(\d+)"', range_input.group(0))
    assert value, "budget range has no value"
    return _markup_brief(role, loc, int(value.group(1)))


def _get(port: int, path: str) -> tuple[int, str]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    conn.request("GET", path, headers={"Accept": "text/html"})
    resp = conn.getresponse()
    body = resp.read()
    if resp.getheader("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    conn.close()
    return resp.status, body.decode("utf-8")


def _slot(html: str, attr: str) -> str:
    m = re.search(rf'<div class="[^"]*" {attr}>\s*(.*?)\s*</div>', html, re.S)
    assert m, f"slot {attr} not served"
    return m.group(1)


@pytest.fixture(scope="module")
def engine_default() -> dict[str, Any]:
    proof_claims.refresh()  # never a cache computed before app registered the engine
    return app._compute_plan_estimate(dict(proof_claims.HUB_PE_DEFAULT_BRIEF))


# ── 1. markup <-> server default brief ───────────────────────────────────────


def test_default_chips_are_the_server_default_brief() -> None:
    assert _default_brief_from_markup() == proof_claims.HUB_PE_DEFAULT_BRIEF


def test_markup_scenarios_are_the_server_grid_definition() -> None:
    """The chips and slider must be exactly what hub_pe_grid() prices: a chip
    with no grid entry would be a dead control, a grid entry with no chip
    wasted startup time."""
    card = _card(HUB.read_text(encoding="utf-8"))
    roles = [
        {"title": c["data-pe-title"], "industry": c["data-pe-industry"]}
        for c in _chips(card, "role")
    ]
    assert roles == [dict(r) for r in proof_claims.HUB_PE_ROLES]
    assert [c["data-pe-location"] for c in _chips(card, "loc")] == list(
        proof_claims.HUB_PE_LOCATIONS
    )
    m = re.search(r"<input[^>]*data-pe-range[^>]*>", card, re.S)
    assert m
    attrs = dict(re.findall(r'(min|max|step|value)="(\d+)"', m.group(0)))
    assert attrs == {
        "min": str(proof_claims.HUB_PE_BUDGET_MIN),
        "max": str(proof_claims.HUB_PE_BUDGET_MAX),
        "step": str(proof_claims.HUB_PE_BUDGET_STEP),
        "value": str(proof_claims.HUB_PE_DEFAULT_BUDGET),
    }


def test_every_chip_carries_the_brief_fields_the_engine_receives() -> None:
    card = _card(HUB.read_text(encoding="utf-8"))
    for role in _chips(card, "role"):
        assert role["data-pe-title"].strip() and role["data-pe-industry"].strip(), role
    for loc in _chips(card, "loc"):
        assert re.fullmatch(r"[A-Z][A-Za-z .]+, [A-Z]{2}", loc["data-pe-location"]), loc
    assert "Illustrative" not in card
    assert "Days to fill" not in card  # the engine has no such figure
    assert "Modeled cost-per-hire at plan efficiency" in re.sub(
        r"<[^>]+>|\{\{[^}]+\}\}", "", card
    )  # the deck's own term
    assert 'autocomplete="off"' in card  # the slider never restores stale browser state
    assert "Nova engine &middot; drag the budget" in card
    assert "data-pe-cph>{{claim_pe_cph}}<" in card
    assert "{{claim_pe_scenario:json}}" in HUB.read_text(encoding="utf-8")


# ── 2. served hub == engine ──────────────────────────────────────────────────


def test_served_card_and_story_4_show_the_engine_default(
    live_port: int, engine_default: dict[str, Any]
) -> None:
    est = engine_default
    cph = int(round(est["est_cph"]))
    status, html = _get(live_port, "/")
    assert status == 200
    # the card's first paint
    assert _slot(html, "data-pe-cph") == f"${cph:,}"
    assert _slot(html, "data-pe-hires") == f"{est['est_hires']:,}"
    assert _slot(html, "data-pe-apps") == f"{est['est_applications']:,}"
    assert _slot(html, "data-pe-cpa") == f"${est['est_cpa']:,.2f}"
    assert _slot(html, "data-pe-basis") == proof_claims.pe_basis_text(est)
    assert "Modeled cost-per-hire at plan efficiency" in re.sub(r"<[^>]+>", "", html)
    for unsourced in ("Nursing median", "NAHS", "SHRM 2024", "SHRM 2025", "US average"):
        assert unsourced not in html, unsourced  # no unsourced or mismatched yardstick
    # the grid URL the page links carries the grid's content hash
    grid = proof_claims.hub_pe_grid()
    assert (
        grid and f'data-pe-grid="/api/hub/prediction-grid?v={grid["version"]}"' in html
    )
    # story 4 echoes the same figure and names the scenario it belongs to
    story = re.search(
        r'<div class="story-moment moment-4">.*?</div>\s*</div>\s*</div>', html, re.S
    )
    assert story, "story 4 not served"
    assert f'data-target="{cph}"' in story.group(0)
    assert f"${cph:,}" in story.group(0)
    b = proof_claims.HUB_PE_DEFAULT_BRIEF
    label = proof_claims.pe_scenario_text(b)
    assert label.replace("\u00a0", " ") == (
        f"{b['roles'][0]} \u00b7 {b['locations'][0]} \u00b7 {b['budget']} budget"
    )
    # the scenario is its own line under the label (typography lens, 2026-10-17)
    parts = proof_claims.pe_scenario_html(b)
    assert (
        f'modeled cost-per-hire<span class="moment-figure-scope">{parts}</span>'
        in story.group(0)
    )
    assert "\u00b7" not in story.group(0)  # the separators are CSS-drawn, never typed
    # the blob hub.js seeds its cache from is the same brief and answer
    blob = re.search(
        r'<script type="application/json" id="pe-default">(.*?)</script>', html, re.S
    )
    assert blob
    assert "</" not in blob.group(1) and "<" not in blob.group(1)
    seed = json.loads(blob.group(1))
    assert seed["brief"] == b
    assert seed["estimate"]["est_cph"] == est["est_cph"]
    assert seed["estimate"]["est_hires"] == est["est_hires"]
    assert seed["estimate"]["channels"] == est["channels"]
    # and nothing of the old model survives
    for stale in (
        "Illustrative model",
        "illustrative example",
        "1,064",
        "Days to fill",
    ):
        assert stale not in html, stale


def test_estimate_endpoint_answers_the_hub_visitor_with_the_served_figure(
    live_port: int, engine_default: dict[str, Any]
) -> None:
    """What the card fetches on a chip click is what the server rendered."""
    status, body = post_json(
        live_port, "/api/estimate", dict(proof_claims.HUB_PE_DEFAULT_BRIEF)
    )
    assert status == 200, body
    assert body["est_cph"] == engine_default["est_cph"]
    assert body["est_hires"] == engine_default["est_hires"]
    assert body["channels"] == engine_default["channels"]


def test_pe_claims_are_engine_output_with_provenance(
    engine_default: dict[str, Any],
) -> None:
    c = proof_claims.claims()
    for key in ("pe_cph", "pe_hires", "pe_apps", "pe_cpa", "pe_scenario"):
        assert key in c, key
        assert "calculate_budget_allocation" in c[key].source
    assert c["pe_basis"].value == int(round(engine_default["cph_floor"]))
    assert c["pe_scenario_html"].text == proof_claims.pe_scenario_html(
        proof_claims.HUB_PE_DEFAULT_BRIEF
    )
    assert "INDUSTRY_CPH_RANGES" in c["pe_basis"].source
    assert "cph_floor_applied" in c["pe_cph"].source  # the audit names the floor
    assert c["pe_grid_version"].text == proof_claims.hub_pe_grid()["version"]
    assert proof_claims.as_dict()["omitted"] == []
    assert c["pe_cph"].value == int(round(engine_default["est_cph"]))
    assert c["pe_hires"].value == engine_default["est_hires"]
    assert c["pe_apps"].value == engine_default["est_applications"]
    assert c["pe_cpa"].text == f"${engine_default['est_cpa']:,.2f}"
    assert c["pe_scenario"].data["brief"] == proof_claims.HUB_PE_DEFAULT_BRIEF
    assert c["pe_scenario"].data["estimate"]["est_cph"] == engine_default["est_cph"]
    assert "/api/estimate" in c["pe_cph"].source


def test_basis_line_states_the_plan_efficiency_case_in_the_decks_terms() -> None:
    """The headline is budget / whole hires priced at the engine's floor (half
    its assumed industry midpoint) -- equal to the floor only when the division
    is exact (trust lens, round 3). The line under it names the floor, the
    whole-hire count and the deck's other case (hires at the midpoint), as the
    deck's "Plan efficiency: $5,250/hire (half the $10.5K midpoint)" and "N at
    range midpoint · M at plan efficiency" do. No external yardstick: SHRM's
    national average is an all-in cost per hire and the card's figure is media
    spend only, so the two never sit together (trust lens, round 2)."""
    est = {
        "cph_floor_applied": True,
        "est_hires": 9,
        "cph_floor": 5250.0,
        "industry_midpoint_cph": 10500.0,
        "est_hires_midpoint": 4,
    }
    text = proof_claims.pe_basis_text(est)
    assert text.replace("\u00a0", " ") == (
        "9 whole hires at the engine\u2019s $5,250 floor \u00b7 4 at its $10,500 midpoint"
    )
    assert " \u00b7\u00a0" in text  # a wrapped line never ends with a stranded dot
    one = proof_claims.pe_basis_text({**est, "est_hires": 1, "est_hires_midpoint": 1})
    assert one.replace("\u00a0", " ").startswith("1 whole hire at")
    for bad in (
        None,
        {**est, "cph_floor_applied": False},
        {**est, "cph_floor": 0},
        {**est, "industry_midpoint_cph": None},
        {**est, "est_hires_midpoint": 0},
        {k: v for k, v in est.items() if k != "est_hires"},
    ):
        with pytest.raises(ValueError):
            proof_claims.pe_basis_text(bad)
    assert (
        "half" not in text.lower()
    )  # the claim never says the headline IS half the midpoint


def test_estimate_carries_the_midpoint_case(engine_default: dict[str, Any]) -> None:
    est = engine_default
    assert est["cph_floor_applied"] is True
    assert est["industry_midpoint_cph"] > est["est_cph"] >= est["cph_floor"] > 0
    assert 1 <= est["est_hires_midpoint"] < est["est_hires"]
    # the plan-efficiency count is the budget at the floor, whole hires; the
    # headline is budget / that count (not the floor itself)
    assert est["cph_floor"] == est["industry_midpoint_cph"] / 2
    assert est["est_hires"] == int(50_000 / est["cph_floor"])
    assert est["est_cph"] == round(50_000 / est["est_hires"], 2)


def test_the_floor_case_holds_in_every_month() -> None:
    """The engine's seasonality follows campaign_start_month (else the wall
    clock), so CI only proves the current month. Every role x market at the
    slider's min / default / max, for every month: at least two hires, the
    floor clamp in force, the printed relation exact."""
    card = _card(HUB.read_text(encoding="utf-8"))
    budgets = _slider_budgets(card)
    for month in range(1, 13):
        for role in proof_claims.HUB_PE_ROLES:
            for loc in proof_claims.HUB_PE_LOCATIONS:
                for budget in budgets:
                    brief = dict(proof_claims.hub_pe_brief(role, loc, budget))
                    brief["campaign_start_month"] = month
                    est = app._compute_plan_estimate(brief)
                    assert est["est_hires"] >= 2, (month, brief, est)
                    assert est["cph_floor_applied"] is True, (month, brief, est)
                    assert est["est_hires"] == int(budget / est["cph_floor"]), (
                        month,
                        brief,
                        est,
                    )
                    assert est["est_cph"] == round(budget / est["est_hires"], 2), (
                        month,
                        brief,
                    )
                    proof_claims.pe_basis_text(est)


def test_json_token_is_script_safe() -> None:
    fake = {
        "pe_scenario": proof_claims.ProofClaim(
            "pe_scenario", None, "x", "test", data={"s": "</script><!--"}
        )
    }
    out = proof_claims.render("{{claim_pe_scenario:json}}", fake)
    assert "<" not in out
    assert json.loads(out) == {"s": "</script><!--"}
    assert proof_claims.render("{{claim_pe_scenario:json}}", {}) == "null"


def test_pe_claims_are_omitted_without_an_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(proof_claims, "_engine", lambda: None)
    monkeypatch.setattr(proof_claims, "_grid", None)  # no cached grid either
    computed = proof_claims.compute()
    # pe_scenario_html is the fixed brief's words, not a figure; everything
    # else the card shows needs the engine and is out
    assert not any(k.startswith("pe_") and k != "pe_scenario_html" for k in computed)
    assert "supply_partners" in computed
    rendered = proof_claims.render(
        "<div data-pe-cph>{{claim_pe_cph}}</div>{{#claim_pe_cph}}X{{/claim_pe_cph}}"
        "{{claim_pe_scenario:json}}",
        computed,
    )
    assert rendered == "<div data-pe-cph>\u2014</div>null"


def test_engine_is_found_whether_app_is_a_module_or_the_main_script(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """gunicorn/wsgi and the tests import app.py as ``app``; the dev server
    runs it as ``__main__`` (``python3 app.py <port>``). The lookup must find
    the engine either way -- the dev server once served the hub with every
    pe_* figure omitted because only ``app`` was checked."""
    import types

    stub = types.ModuleType("__main__")
    stub._compute_plan_estimate = lambda brief: {"est_hires": 1}  # type: ignore[attr-defined]
    monkeypatch.delitem(sys.modules, "app")
    monkeypatch.setitem(sys.modules, "__main__", stub)
    assert proof_claims._engine() is stub._compute_plan_estimate
    monkeypatch.delitem(sys.modules, "__main__")
    assert proof_claims._engine() is None
    monkeypatch.setitem(sys.modules, "app", app)
    assert proof_claims._engine() is app._compute_plan_estimate


def test_claims_cached_before_the_engine_loaded_are_recomputed_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """nova.py can render a greeting (and cache the claims) before app.py is
    imported; the first read after the engine appears must pick the pe_*
    claims up, and only that once."""
    real_engine = proof_claims._engine
    monkeypatch.setattr(proof_claims, "_engine", lambda: None)
    before = proof_claims.refresh()
    assert "pe_cph" not in before
    monkeypatch.setattr(proof_claims, "_engine", real_engine)
    after = proof_claims.claims()
    assert "pe_cph" in after and after is not before
    assert proof_claims.claims() is after  # no second recompute


# ── 3. the whole grid is engine-priced and sane ─────────────────────────────


def test_whole_grid_is_sane_and_every_funded_channel_has_a_row() -> None:
    """Every scenario a visitor can reach: at least two hires (never the whole
    budget shown as a cost per hire), a cost per hire the budget affords,
    applications, and only channels that have a row; and for one role in one
    market, hires never fall as the budget rises. Found the $25K tech-in-New-
    York cliff (1 hire, $25,000 cost per hire) that set the slider floor."""
    grid = proof_claims.hub_pe_grid(force=True)
    assert grid is not None and not grid["skipped"], grid and grid["skipped"]
    budgets = proof_claims.hub_pe_budgets()
    assert len(grid["scenarios"]) == (
        len(proof_claims.HUB_PE_ROLES)
        * len(proof_claims.HUB_PE_LOCATIONS)
        * len(budgets)
    )
    card = _card(HUB.read_text(encoding="utf-8"))
    rows = set(re.findall(r'data-pe-ch="([a-z_]+)"', card))
    funded: set[str] = set()
    for role in proof_claims.HUB_PE_ROLES:
        for loc in proof_claims.HUB_PE_LOCATIONS:
            last_hires = 0
            for budget in budgets:
                key = proof_claims.hub_pe_key(
                    proof_claims.hub_pe_brief(role, loc, budget)
                )
                est = grid["scenarios"][key]
                assert est["est_hires"] >= 2, (key, est)
                assert est["est_cph"] and 0 < est["est_cph"] <= budget / 2, (key, est)
                assert est["est_applications"] > 0 and est["est_cpa"] > 0, (key, est)
                assert est["est_hires"] >= last_hires, (key, est, last_hires)
                last_hires = est["est_hires"]
                # "at plan efficiency" must be true for every reachable scenario,
                # and the basis line must have both of its figures
                assert est["cph_floor_applied"] is True, key
                assert 1 <= est["est_hires_midpoint"] <= est["est_hires"], (key, est)
                assert est["industry_midpoint_cph"] > est["est_cph"], (key, est)
                # the printed relation holds arithmetically for every scenario:
                # hires = int(budget / floor), headline = budget / hires >= floor,
                # floor = half the midpoint, midpoint hires = int(budget / midpoint)
                assert est["cph_floor"] == est["industry_midpoint_cph"] / 2, (key, est)
                assert est["est_hires"] == int(budget / est["cph_floor"]), (key, est)
                assert est["est_cph"] == round(budget / est["est_hires"], 2), (key, est)
                assert est["est_cph"] >= est["cph_floor"] - 0.01, (key, est)
                assert est["est_hires_midpoint"] == int(
                    budget / est["industry_midpoint_cph"]
                ), (key, est)
                proof_claims.pe_basis_text(est)
                keys = {c["key"] for c in est["channels"]}
                assert keys <= rows, f"{key}: engine funds {keys - rows} with no row"
                assert abs(sum(c["amount"] for c in est["channels"]) - budget) < 1, key
                funded |= keys
    assert funded == rows, f"rows the engine never funds: {rows - funded}"
    assert grid["default"] in grid["scenarios"]


def test_grid_endpoint_serves_the_scenarios_publicly_and_cacheably(
    live_port: int, engine_default: dict[str, Any]
) -> None:
    conn = http.client.HTTPConnection("127.0.0.1", live_port, timeout=120)
    version = proof_claims.hub_pe_grid()["version"]
    conn.request(
        "GET",
        f"/api/hub/prediction-grid?v={version}",
        headers={"Accept-Encoding": "gzip"},
    )
    resp = conn.getresponse()  # no Origin, no cookie: public
    body = resp.read()
    assert resp.getheader("Content-Encoding") == "gzip" and len(body) < 60_000, len(
        body
    )
    body = gzip.decompress(body)
    etag = resp.getheader("ETag")
    cache = resp.getheader("Cache-Control") or ""
    conn.close()
    assert resp.status == 200, body[:200]
    assert etag and "public" in cache and "max-age=86400" in cache
    grid = json.loads(body)
    # a URL for another version (an old page, or an old instance mid-deploy)
    # still gets the current grid but is never cached under that URL
    conn = http.client.HTTPConnection("127.0.0.1", live_port, timeout=120)
    conn.request("GET", "/api/hub/prediction-grid?v=0000000000000000dead")
    resp = conn.getresponse()
    other = json.loads(resp.read())
    assert resp.status == 200 and resp.getheader("Cache-Control") == "no-store"
    assert other["version"] == version
    conn.close()
    assert etag == f'"{grid["version"]}"' and len(grid["version"]) == 20
    assert grid["version"] == proof_claims.hub_pe_grid()["version"]
    default = grid["scenarios"][grid["default"]]
    assert default["est_cph"] == engine_default["est_cph"]
    assert default["est_hires"] == engine_default["est_hires"]
    assert default["channels"] == engine_default["channels"]
    assert grid["default"] == proof_claims.hub_pe_key(proof_claims.HUB_PE_DEFAULT_BRIEF)
    assert "<" not in body.decode("utf-8")  # nothing a page could mis-parse
    conn = http.client.HTTPConnection("127.0.0.1", live_port, timeout=120)
    conn.request("GET", "/api/hub/prediction-grid", headers={"If-None-Match": etag})
    resp = conn.getresponse()
    resp.read()
    conn.close()
    assert resp.status == 304


# ── 4. hub.js drives the card from the engine grid ──────────────────────────


def test_hub_js_drives_the_card_from_the_engine_grid_and_carries_no_constants() -> None:
    js = HUB_JS.read_text(encoding="utf-8")
    block = js[js.index("PREDICTION ENGINE") :]
    block = block[: block.index("})();") + 5]
    assert 'card.getAttribute("data-pe-grid")' in block  # the hashed URL
    assert '"/api/hub/prediction-grid"' in block
    assert 'getElementById("pe-default")' in block
    assert "gridLoaded" in block  # a scenario missing from the grid must never refetch
    for attr in ("data-pe-industry", "data-pe-title", "data-pe-location", "data-pe-ch"):
        assert attr in block, attr
    for relic in (
        "var ROLES",
        "var LOCS",
        "ath:",
        "cpa: 18",
        "days:",
        "pePinned",
        "loc.m",
        "cph_benchmark",  # the per-industry quartiles are unsourced; never shown
        '"/api/estimate"',  # no authenticated, rate-limited request per drag
        'method: "POST"',
    ):
        assert relic not in block, relic
    # the scenario key hub.js builds is the one hub_pe_grid() keys by
    assert 'b.roles[0] + "|" + b.locations[0] + "|" + b.budget' in block
    assert proof_claims.hub_pe_key(proof_claims.HUB_PE_DEFAULT_BRIEF) == (
        "Registered Nurse|Houston, TX|$50,000"
    )
