"""The wizard preview, /api/estimate and /api/generate read a budget the SAME
way (wizard audit D-02, 2026-10-01).

Pre-fix: typing "1.5 million" previewed $1.5M (the preview's parseMoney read
the letter m) while the server planned $1.50 -- shared_utils.parse_budget
only read "1.5" -- and still shipped a 0-hire bundle (server log:
"Budget allocation: parsed '1.5 million' -> $1.50"). "$5 million" -> $5,
"1,5M" -> $15, "10 000" -> $10, "10000-15000" previewed $10,000 but planned
$12,500, "1.5.2M" crashed /api/estimate with a 500.

Now wizard_inputs.parse_budget_input is the one reading; the page runs its
JS port (templates/partials/index/body_inputs_js.html) with the server's
tables embedded. Both implementations are held to ONE golden table,
tests/fixtures/budget_input_golden.json (the spec -- fix code, not rows).
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

import app
import plan_currency
import template_composer
import wizard_inputs
from shared_utils import parse_budget
from tests.live_server import live_port, post_json  # noqa: F401 (fixture)
from tests.wizard_js_harness import NODE, inputs_functions_js, run_node

GOLDEN = json.loads(
    (Path(__file__).parent / "fixtures" / "budget_input_golden.json").read_text(
        encoding="utf-8"
    )
)
PARSE_ROWS = GOLDEN["parse"]
PLAN_ROWS = GOLDEN["plan"]
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")


def _expected_parse(row: dict) -> dict:
    if not row["ok"]:
        return {"ok": False, "error": row["error"]}
    amount = float(row["amount"])
    return {
        "ok": True,
        "amount": amount,
        "low": float(row.get("low", amount)),
        "high": float(row.get("high", amount)),
        "is_range": bool(row.get("is_range", False)),
        "period": row.get("period", ""),
        "currency": row.get("currency", ""),
    }


def _actual_parse(result: dict) -> dict:
    if not result["ok"]:
        return {"ok": False, "error": result["error"]}
    return {
        k: (float(result[k]) if k in ("amount", "low", "high") else result[k])
        for k in ("ok", "amount", "low", "high", "is_range", "period", "currency")
    }


def _row_id(row: dict) -> str:
    return repr(row["input"])[:40]


# ── the table itself ─────────────────────────────────────────────────────


def test_golden_table_is_broad():
    assert len(PARSE_ROWS) >= 60
    inputs = [r["input"] for r in PARSE_ROWS]
    for must in ("1.5 million", "$5 million", "1,5M", "10 000", "10000-15000",
                 "1e9", "$", "", None, "1.5.2M", "50%"):
        assert must in inputs, must
    errors = {r["error"] for r in PARSE_ROWS if not r["ok"]}
    assert {"empty", "not_a_number", "exponent", "percent", "malformed_number",
            "negative", "zero", "multiple_amounts", "implausible_range",
            "unsupported_period", "conflicting_period", "too_long"} <= errors
    too_long = [r for r in PARSE_ROWS if r.get("error") == "too_long"][0]
    assert len(too_long["input"]) > wizard_inputs.INPUT_LIMITS["budget_max_chars"]


# ── server ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("row", PARSE_ROWS, ids=_row_id)
def test_server_parse_matches_golden(row):
    got = wizard_inputs.parse_budget_input(row["input"]).as_dict()
    assert _actual_parse(got) == _expected_parse(row)
    if not got["ok"]:
        assert got["message"] == wizard_inputs.BUDGET_ERROR_MESSAGES[row["error"]]


@pytest.mark.parametrize("row", PLAN_ROWS, ids=lambda r: f"{r['input']}|{r['period']}|{r['duration']}")
def test_server_plan_budget_matches_golden(row):
    plan = wizard_inputs.resolve_plan_budget(row["input"], row["period"], row["duration"])
    assert plan.ok is row["ok"], plan
    if row["ok"]:
        assert plan.total == float(row["total"])
        assert plan.months == float(row["months"])
        assert plan.multiplier == float(row["multiplier"])
    else:
        assert plan.error == row["error"]
        assert plan.message == wizard_inputs.BUDGET_ERROR_MESSAGES[row["error"]]


# ── the page's JS port, same table ───────────────────────────────────────


def _js_results(rows: list, expr: str) -> list:
    script = "\n".join(
        [
            inputs_functions_js(),
            "var rows = " + json.dumps(rows) + ";",
            "process.stdout.write(JSON.stringify(rows.map(function (r) {",
            "  return " + expr + ";",
            "})));",
        ]
    )
    return run_node(script)


@needs_node
def test_js_parse_matches_golden_row_for_row():
    got = _js_results(PARSE_ROWS, "novaParseBudget(r.input)")
    mismatches = [
        (row["input"], _actual_parse(res), _expected_parse(row))
        for row, res in zip(PARSE_ROWS, got)
        if _actual_parse(res) != _expected_parse(row)
        or (not res["ok"] and res["message"] != wizard_inputs.BUDGET_ERROR_MESSAGES[row["error"]])
    ]
    assert not mismatches, mismatches


@needs_node
def test_js_plan_budget_matches_golden_row_for_row():
    got = _js_results(
        PLAN_ROWS, "novaResolvePlanBudget(r.input, r.period, r.duration)"
    )
    mismatches = []
    for row, res in zip(PLAN_ROWS, got):
        if res["ok"] is not row["ok"]:
            mismatches.append((row, res))
        elif row["ok"] and (
            res["total"] != float(row["total"])
            or res["months"] != float(row["months"])
            or res["multiplier"] != float(row["multiplier"])
        ):
            mismatches.append((row, res))
        elif not row["ok"] and res["error"] != row["error"]:
            mismatches.append((row, res))
    assert not mismatches, mismatches


@needs_node
def test_js_and_server_agree_beyond_the_table():
    # Every row again, plus mutations (case, padding, currency, suffix
    # spacing): the two implementations must agree on each, bit for bit.
    extra = []
    for row in PARSE_ROWS:
        s = row["input"]
        if isinstance(s, str) and s:
            extra += [s.upper(), f"  {s}  ", f"${s}", f"{s} USD", s.replace(" ", "")]
    rows = [{"input": s} for s in extra]
    js = _js_results(rows, "novaParseBudget(r.input)")
    for s, res in zip(extra, js):
        py = wizard_inputs.parse_budget_input(s).as_dict()
        assert _actual_parse(res) == _actual_parse(py), s


def _server_currency(raw, period, duration, locations):
    """plan_currency reading exactly as /api/estimate returns it."""
    brief = {"budget_range": raw, "budget_period": period,
             "campaign_duration": duration, "locations": locations}
    plan = app._resolve_request_budget(brief)
    app._normalize_request_budget(brief, log=False)
    return app._plan_currency_reading(brief, plan)


@needs_node
def test_budget_field_and_review_state_the_planned_figure():
    """The line under the budget field and the review step's Budget row
    (novaBudgetReadingText) say which figure the plan uses -- range midpoint,
    per-period scaling -- in the PLAN's currency as the server resolves it,
    with a plain note when what was typed is not that currency."""
    dallas, london, mumbai = ["Dallas, TX"], ["London, UK"], ["Mumbai, India"]
    cases = [
        ("1.5 million", "campaign", "3 months", dallas),
        ("10000-15000", "campaign", "3 months", dallas),
        ("$10,000", "monthly", "6-12 months", dallas),
        ("60,000", "quarterly", "3-6 months", dallas),
        ("120k", "annual", "3 months", dallas),
        ("€50.000,50", "campaign", "3 months", dallas),
        ("50k/mo", "campaign", "3 months", dallas),
        ("EUR 50,000", "campaign", "3 months", dallas),
        ("50,000 euros", "campaign", "3 months", dallas),
        ("Rs 12,50,000", "campaign", "3 months", dallas),
        ("Rs 12,50,000", "campaign", "3 months", mumbai),
        ("50,000", "campaign", "3 months", london),
        ("CA$50,000", "campaign", "3 months", ["Toronto, ON"]),
    ]
    rows = [
        {"input": c[0], "period": c[1], "duration": c[2],
         "cur": _server_currency(*c)}
        for c in cases
    ]
    got = _js_results(
        rows,
        "novaBudgetReadingText(novaResolvePlanBudget(r.input, r.period, r.duration), r.cur)",
    )
    assert got == [
        "Planning at $1,500,000",
        "Planning at $12,500 (midpoint of $10,000\u2013$15,000)",
        "Planning at $90,000 total: $10,000 per month \u00d7 9 months",
        "Planning at $90,000 total: $60,000 per quarter \u00d7 1.5 quarters (4.5-month campaign)",
        "Planning at $120,000 total: $120,000 per year \u00d7 1 year (3-month campaign;"
        " never scaled below the amount typed)",
        "Planning at \u20ac50,000.50",
        "Planning at $50,000. Your amount says per month; set the period to"
        " \u201cPer month\u201d if that's what you mean.",
        "Planning at $50,000 \u2014 plan currency is USD for your locations;"
        " \u201cEUR\u201d isn\u2019t read as a currency symbol",
        "Planning at $50,000 \u2014 plan currency is USD for your locations;"
        " \u201cEUROS\u201d isn\u2019t read as a currency symbol",
        "Planning at $1,250,000 \u2014 plan currency is USD for your locations;"
        " \u201cRS\u201d isn\u2019t read as a currency symbol",
        "Planning at \u20b91,250,000",  # typed Rs == plan INR: nothing to flag
        "Planning at \u00a350,000 \u2014 plan currency is GBP for your locations",
        "Planning at C$50,000",
    ]
    # the server plans exactly the figure each line states, in that currency
    for (raw, period, duration, locs), line, row in zip(cases, got, rows):
        total = wizard_inputs.resolve_plan_budget(raw, period, duration).total
        assert line.startswith("Planning at " + row["cur"]["symbol"].strip())
        assert f"{total:,.2f}".rstrip("0").rstrip(".") in line


def test_plan_currency_reading_uses_the_one_resolver():
    import plan_currency

    for raw, locs in [("EUR 50,000", ["Dallas, TX"]), ("\u00a350,000", ["Dallas, TX"]),
                      ("50,000", ["London, UK"]), ("CA$50,000", ["Toronto, ON"]),
                      ("$50,000", ["London, UK", "Berlin, Germany"])]:
        reading = _server_currency(raw, "campaign", "3 months", locs)
        brief = {"budget_range": raw, "locations": locs}
        app._normalize_request_budget(brief, log=False)
        assert (reading["code"], reading["basis"]) == \
            plan_currency.currency_for_plan_with_basis(brief), raw


def test_estimate_budget_only_mode_returns_the_plan_currency(live_port):  # noqa: F811
    status, body = post_json(
        live_port, "/api/estimate",
        {"budget_range": "EUR 50,000", "locations": ["Dallas, TX"], "budget_only": True},
    )
    assert status == 200, body
    assert body["budget"]["total"] == 50000
    assert body["plan_currency"]["code"] == "USD"
    assert "EUR" in body["plan_currency"]["note"]
    assert "est_hires" not in body  # no engine call, no roles needed


@needs_node
def test_js_and_server_agree_on_generated_inputs():
    """Differential check on 4,000 generated budget strings (fixed seed):
    digits, every separator, magnitude words, currency tokens, period
    markers, range words and junk, in random order. The two parsers must
    return the same result for every one -- the golden table covers intent,
    this covers the long tail of the port."""
    import random

    rng = random.Random(20261001)
    pieces = [
        "1", "15", "150", "1,500", "1.500", "1 500", "1'500", "0", "00", "2,50,000",
        "10.000,50", "1.5", "1,5", ".", ",", " ", "  ", "-", "–", "to", "and",
        "between", "k", "K", "m", "M", "mm", "mn", "b", "bn", "thousand", "million",
        "billion", "lakh", "crore", "cr", "$", "US$", "€", "£", "₹", "EUR",
        "usd", "Rs.", "kr", "/mo", "per month", "monthly", "per quarter", "p.a.",
        "annual", "per week", "daily", "~", "<", ">", "+", "up to", "approx",
        "budget:", "total", "%", "e", "1e9", "(", ")", "x", "abc", " ", " ",
        "５", "١", "0,750", "0.750", "CA$", "AU$", "NT$", "٫", "٬",
        "१", "৫", "16666.666666666668", ".5",
    ]
    inputs = []
    for _ in range(4000):
        n = rng.randint(1, 6)
        inputs.append("".join(rng.choice(pieces) for _ in range(n)))
    js = _js_results([{"input": s} for s in inputs], "novaParseBudget(r.input)")
    diffs = []
    for s, res in zip(inputs, js):
        py = wizard_inputs.parse_budget_input(s).as_dict()
        if _actual_parse(res) != _actual_parse(py):
            diffs.append((s, _actual_parse(py), _actual_parse(res)))
    assert not diffs, diffs[:10]


# ── API clients: every budget shape the suite posts behaves as on origin/main ─


# Shapes taken from the payloads the existing tests post (number, int, float
# with noise, $/£/₹/A$/GBP strings, "120k", "... total").
_API_SHAPES = [
    20000, 50000, 100000, 2_000_000, 8000, 50000.0, 110000.00000000001, 50000 / 3,
    "$120,000", "120000", "120k", "$90,000", "$150,000", "$1,000,000", "$1,200",
    "£50,000", "£50,000 total", "₹2,50,00,000", "₹9,00,000 total", "A$80,000",
    "GBP 2,000,000", "¥5,000,000", "$250,000", "150000", "250000",
]


@pytest.mark.parametrize("shape", _API_SHAPES, ids=repr)
def test_api_budget_shapes_resolve_as_on_origin_main(shape):
    """origin/main read these with shared_utils.parse_budget on both
    endpoints; JSON numbers reach /api/estimate as numbers and /api/generate
    as str() (the request sanitizer). Float noise (110000.00000000001,
    50000/3) briefly 400'd on generate after the first parity commit; every
    shape must resolve to origin/main's amount (to the cent) both ways."""
    legacy = parse_budget(shape)
    for data in ({"budget": shape}, app._sanitize_generate_request({"budget": shape})):
        plan = app._resolve_request_budget(dict(data, budget_period="campaign"))
        assert plan.ok, (shape, data, plan.error)
        assert plan.total == pytest.approx(round(legacy, 2), abs=0.005), (shape, data)


def test_float_noise_budget_estimate_endpoint(live_port):  # noqa: F811
    for shape in (110000.00000000001, 50000 / 3 * 6):
        status, body = post_json(
            live_port, "/api/estimate", dict(_EST_BRIEF, budget=shape, budget_range=None)
        )
        assert status == 200, (shape, body)
        assert body["budget"]["total"] == round(shape, 2)


# ── what downstream readers see ──────────────────────────────────────────


@pytest.mark.parametrize("row", [r for r in PARSE_ROWS if r["ok"]], ids=_row_id)
def test_canonical_budget_string_reads_back_identically(row):
    """/api/generate rewrites the budget to a canonical string; every
    downstream reader (engine, workbook, deck, Slack) re-reads it with the
    legacy shared_utils.parse_budget -- which must now get the same number,
    and plan_currency must see the same declared currency as for the raw
    text."""
    data = {"budget_range": row["input"], "budget_period": "campaign",
            "campaign_duration": "3 months"}
    plan = app._resolve_request_budget(data)
    if not plan.ok:  # parses, but outside the plan bounds (e.g. "2B", "12,34")
        assert plan.error in ("too_small", "too_large")
        return
    app._normalize_request_budget(data)
    assert parse_budget(data["budget"]) == pytest.approx(plan.total, abs=0.005)
    assert data["budget_range"] == data["budget"]
    raw = str(row["input"])
    assert plan_currency.currency_codes_from_symbol(
        data["budget"]
    ) == plan_currency.currency_codes_from_symbol(raw)


def test_generate_budget_block_plans_the_parsed_amount():
    """The /api/generate budget block (run from app.py source, as the
    period-parity test does) hands generation 1.5M for "1.5 million" --
    pre-fix it left the raw text, which every consumer read as $1.50."""
    from tests.test_budget_period_preview_parity import _server_campaign_budget

    assert _server_campaign_budget("1.5 million", "campaign", "3 months") == 1_500_000
    assert _server_campaign_budget("$5 million", "campaign", "3 months") == 5_000_000
    assert _server_campaign_budget("1,5M", "campaign", "3 months") == 1_500_000
    assert _server_campaign_budget("10 000", "campaign", "3 months") == 10_000
    assert _server_campaign_budget("10000-15000", "campaign", "3 months") == 12_500
    assert _server_campaign_budget("1.5 million", "monthly", "6-12 months") == 13_500_000


# ── the page gets the server's tables ────────────────────────────────────


def test_wizard_page_embeds_the_server_tables():
    html = template_composer.compose_template("index").decode("utf-8")
    assert template_composer.WIZARD_INPUTS_PLACEHOLDER not in html
    start = html.index('<script type="application/json" id="novaWizardInputs">')
    start = html.index(">", start) + 1
    embedded = json.loads(html[start : html.index("</script>", start)])
    assert embedded == json.loads(template_composer.wizard_inputs_json())
    assert embedded["limits"] == wizard_inputs.INPUT_LIMITS
    assert embedded["budget"]["regex"] == wizard_inputs.BUDGET_REGEX
    # the input-rules script runs before the wizard and preview scripts use it
    assert html.index("function novaParseBudget(") < html.index("function validateBudgetInput(")
    assert html.index("function novaParseBudget(") < html.index("function gather()")


# ── the endpoints ────────────────────────────────────────────────────────


_EST_BRIEF = {
    "industry": "blue_collar_trades",
    "client_name": "Acme Fabrication",
    "target_roles": ["Welder"],
    "locations": ["Houston, TX"],
    "budget_period": "campaign",
    "campaign_duration": "3 months",
    "target_region": "us_only",
}


def test_estimate_plans_1_5_million_as_1_5_million():
    est = app._compute_plan_estimate(dict(_EST_BRIEF, budget_range="1.5 million"))
    assert est["est_hires"] > 100, est  # pre-fix: 0 hires on $1.50
    assert est["budget"]["total"] == 1_500_000
    assert est["budget"]["canonical"] == "1,500,000"


def test_estimate_scales_the_period_itself():
    # The preview now posts the raw amount + period + duration; the server
    # scales it -- $10,000/month over "6-12 months" plans $90,000.
    est = app._compute_plan_estimate(
        dict(_EST_BRIEF, budget_range="$10,000", budget_period="monthly",
             campaign_duration="6-12 months")
    )
    assert est["budget"]["total"] == 90_000
    assert est["budget"]["months"] == 9
    assert abs(sum(c["amount"] for c in est["channels"]) - 90_000) < 1


@pytest.mark.parametrize(
    "budget,error",
    [("1.5.2M", "malformed_number"), ("abc", "not_a_number"), ("-5", "negative"),
     ("1.5", "too_small"), ("2B", "too_large"), ("50%", "percent"), ("1e9", "exponent")],
)
def test_estimate_rejects_non_amounts_with_the_field_named(budget, error):
    with pytest.raises(app._EstimateValidationError) as exc:
        app._compute_plan_estimate(dict(_EST_BRIEF, budget_range=budget))
    assert str(exc.value) == wizard_inputs.BUDGET_ERROR_MESSAGES[error]
    assert exc.value.field == "budget_range"


@pytest.mark.parametrize(
    "budget,error",
    [("1.5.2M", "malformed_number"), ("abc", "not_a_number"), ("2B", "too_large"),
     ("0", "zero"), ("-$50,000", "negative")],
)
def test_generate_rejects_non_amounts_with_the_field_named(live_port, budget, error):  # noqa: F811
    status, body = post_json(
        live_port,
        "/api/generate",
        {
            "requester_name": "QA Bot",
            "requester_email": "qa@joveo.com",
            "client_name": f"Budget Parse Co {budget}",
            "use_case": "Hiring welders",
            "target_roles": ["Welder"],
            "locations": ["Houston, TX"],
            "budget_range": budget,
            "budget_period": "campaign",
            "campaign_duration": "3 months",
        },
    )
    assert status == 400, (status, body)
    assert body["field"] == "budget_range", body
    assert body["error"] == wizard_inputs.BUDGET_ERROR_MESSAGES[error]


def test_estimate_endpoint_400_names_the_field(live_port):  # noqa: F811
    status, body = post_json(
        live_port, "/api/estimate", dict(_EST_BRIEF, budget_range="1.5.2M")
    )
    assert status == 400, (status, body)  # pre-fix: 500 "Estimate calculation failed"
    assert body["field"] == "budget_range"


def test_amounts_never_become_nan_or_inf():
    for row in PARSE_ROWS:
        res = wizard_inputs.parse_budget_input(row["input"])
        assert math.isfinite(res.amount)
