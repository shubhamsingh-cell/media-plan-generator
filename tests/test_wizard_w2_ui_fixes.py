"""Wizard fixes from the 2026-10-16 re-verification of the UI audit.

D-07  Export PDF sent no CSRF token (403 -> silent window.print()); CSV/XLSX
      exported the wizard REQUEST (header-only CSV, "0 channels" workbook).
D-13  Example chips silently overwrote client/budget/industry and kept the
      typed role beside the example's.
D-14  saveFormState persisted no region, channel, section or job-category
      keys, so a reload silently reverted them.
D-15  At <=1080px the live preview sat ABOVE step 1 (`order: -1`).
D-16  The step-5 button row had no wrap rule: labels clipped at every width.
D-24  Tag "remove" was a bare span, alertdialogs lacked aria-modal, no <h1>.
Copy  The past-campaign upload implied the data calibrates the plan; nothing
      reads it.

Front-end behaviour is exercised by lifting the REAL functions out of the
templates and running them under node against a stub DOM (see
tests/wizard_js_harness.py); the routes are driven with a handler double.
"""

from __future__ import annotations

import csv
import io
import json
import re
import sys
import types

import pytest

from tests.wizard_js_harness import (
    APP_JS,
    NODE,
    PARTIALS,
    PREVIEW_JS,
    js_block,
    run_node,
    slice_between,
)

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")

BODY = (PARTIALS / "body_content.html").read_text(encoding="utf-8")
FOOTER = (PARTIALS / "body_footer_scripts.html").read_text(encoding="utf-8")
# Comments stripped: the rules are explained next to themselves, and a comment
# that mentions `order: -1` or an <h1> must not satisfy (or trip) a rule test.
STYLES = re.sub(
    r"/\*.*?\*/", "", (PARTIALS / "head_styles.html").read_text(encoding="utf-8"), flags=re.S
)
APP = APP_JS.read_text(encoding="utf-8")
PREVIEW = PREVIEW_JS.read_text(encoding="utf-8")
META = (PARTIALS / "head_meta.html").read_text(encoding="utf-8")
A11Y = (PARTIALS / "body_a11y_js.html").read_text(encoding="utf-8")


# ══════════════════════════════════════════════════════════════════════════
# Routes: PDF + CSV/XLSX (D-07)
# ══════════════════════════════════════════════════════════════════════════


class _Handler:
    """Just enough of the BaseHTTPRequestHandler the export routes touch."""

    def __init__(self, body: dict):
        raw = json.dumps(body).encode()
        self.headers = {"Content-Length": str(len(raw))}
        self.rfile = io.BytesIO(raw)
        self.wfile = io.BytesIO()
        self.status = None
        self.sent = {}
        self.json: dict | None = None

    def send_response(self, code):
        self.status = code

    def send_header(self, key, value):
        self.sent[key] = value

    def end_headers(self):
        pass

    def _send_json(self, obj, status_code=200):
        self.status = status_code
        self.json = obj

    def error(self) -> str:
        """The "error" text of the JSON the route sent (a missing JSON body
        fails here with a clear message, not a TypeError on None)."""
        assert self.json is not None, f"no JSON was sent (status {self.status})"
        return str(self.json["error"])

    @property
    def body(self) -> bytes:
        return self.wfile.getvalue()


PLAN_ID = "a" * 32

STORED_PLAN = {
    "channels": [
        {
            "name": "Niche Boards",
            "allocation_pct": 60.0,
            "budget": 36000.0,
            "cpc_range": "$1.40",
            "cpa_range": "$12.73",
            "projected_clicks": 25714,
            "recommended_reason": "",
        },
        {
            "name": "Programmatic Dsp",
            "allocation_pct": 40.0,
            "budget": 24000.0,
            "cpc_range": "$1.02",
            "cpa_range": "$15.47",
            "projected_clicks": 23529,
            "recommended_reason": "=HYPERLINK(\"http://x\")",
        },
    ],
    "budget_summary": {"total": 60000.0},
    "recommendations": ["Lead with Niche Boards."],
    "metadata": {
        "roles": ["Registered Nurse"],
        "locations": ["Austin, TX"],
        "industry": "Healthcare",
        "total_budget": 60000.0,
    },
}

# What the wizard posts: its own request, no channels anywhere.
WIZARD_REQUEST = {
    "client_name": "Acme Health",
    "industry": "healthcare_medical",
    "budget_range": "60000",
    "target_roles": ["Registered Nurse"],
    "locations": ["Austin, TX"],
}


@pytest.fixture
def export(monkeypatch):
    from routes import export as ex

    monkeypatch.setattr(ex, "_track_export_event", lambda *_a, **_k: None)
    # raising=False: on the pre-fix tree the helper does not exist, and the
    # tests below must fail on the route's BEHAVIOUR, not on this fixture.
    monkeypatch.setattr(
        ex,
        "_stored_plan_json",
        lambda pid: STORED_PLAN if pid == PLAN_ID else None,
        raising=False,
    )
    return ex


def _post_sheets(ex, body):
    h = _Handler(body)
    ex._handle_export_sheets(h, "/api/export/sheets", None)
    return h


def test_csv_export_is_the_generated_plan(export):
    h = _post_sheets(
        export,
        {
            "plan_id": PLAN_ID,
            "format": "csv",
            "client_name": "Acme Health",
            "industry_label": "Healthcare",
        },
    )
    assert h.status == 200
    assert h.sent["Content-Type"].startswith("text/csv")
    assert "Acme_Health_Media_Plan_Export.csv" in h.sent["Content-Disposition"]
    rows = list(csv.reader(io.StringIO(h.body.decode("utf-8-sig"))))
    assert ["Client", "Acme Health"] in rows
    assert ["Total campaign budget", "60000.0"] in rows
    assert ["Funded channels", "2"] in rows
    # funded channels, workbook display names, dollar amounts
    assert ["Niche / Industry Boards", "60.0", "36000.0", "$1.40", "$12.73", "25714", ""] in rows
    # a value that starts with = cannot run as a spreadsheet formula
    programmatic = next(r for r in rows if r and r[0] == "Programmatic (DSP)")
    assert programmatic[-1].startswith("'=")
    assert ["Total", "100.0", "60000.0", "", "", "49243", ""] in rows
    assert ["1", "Lead with Niche Boards."] in rows


def test_xlsx_export_is_the_generated_plan(export):
    openpyxl = pytest.importorskip("openpyxl")
    h = _post_sheets(
        export,
        {"plan_id": PLAN_ID, "format": "xlsx", "client_name": "Acme Health"},
    )
    assert h.status == 200
    assert "spreadsheetml" in h.sent["Content-Type"]
    wb = openpyxl.load_workbook(io.BytesIO(h.body))
    assert wb.sheetnames == ["Summary", "Channels", "Recommendations"]
    ws = wb["Channels"]
    assert ws.max_row == 4  # header + 2 channels + total
    assert ws["A2"].value == "Niche / Industry Boards"
    assert ws["C2"].value == 36000.0  # a number, not "$36,000" text
    assert ws["F4"].value == 49243


@pytest.mark.parametrize("fmt", ["csv", "xlsx"])
def test_file_export_refuses_the_wizard_request(export, fmt):
    """Pre-fix: 200 and a header-only CSV / a workbook with 0 channels."""
    h = _post_sheets(export, {"plan_data": WIZARD_REQUEST, "format": fmt})
    assert h.status == 400, h.status
    assert "Nothing to export" in h.error()
    assert h.body == b""


def test_file_export_unknown_and_malformed_plan_ids(export):
    unknown = _post_sheets(export, {"plan_id": "b" * 32, "format": "csv"})
    assert unknown.status == 404
    assert "expired" in unknown.error()
    malformed = _post_sheets(export, {"plan_id": "../../etc/passwd", "format": "xlsx"})
    assert malformed.status == 400


def test_file_export_still_accepts_a_posted_finished_plan(export):
    """API callers that already hold a plan (channels present) keep working."""
    h = _post_sheets(
        export,
        {
            "plan_data": {
                "client_name": "Acme",
                "budget": "1000",
                "channels": [{"name": "LinkedIn", "budget": 1000}],
            },
            "format": "csv",
        },
    )
    assert h.status == 200
    assert b"LinkedIn" in h.body


def test_stored_plan_lookup_reads_apps_store_and_honours_the_24h_life(monkeypatch):
    from routes import export as ex
    import time

    entries = {
        "c" * 32: {"data": {"channels": [1]}, "created": time.time()},
        "d" * 32: {"data": {"channels": [1]}, "created": time.time() - 90000},
    }
    fake_app = types.SimpleNamespace(_plan_result_lookup=lambda pid: entries.get(pid))
    monkeypatch.setitem(sys.modules, "app", fake_app)
    assert ex._stored_plan_json("c" * 32) == {"channels": [1]}
    assert ex._stored_plan_json("d" * 32) is None
    assert ex._stored_plan_json("e" * 32) is None


def test_stored_pdf_download_answers_503_when_the_plan_store_is_missing(monkeypatch):
    """No app module / no lookup helper: a clear 503, not an AttributeError
    surfacing as a generic 500."""
    from routes import export as ex

    monkeypatch.setitem(sys.modules, "app", types.SimpleNamespace())
    h = _Handler({})
    parsed = types.SimpleNamespace(query="plan_id=" + "a" * 32)
    ex._handle_pdf_export_get(h, "/api/plan/export/pdf", parsed)
    assert h.status == 503
    assert "unavailable" in h.error()


def test_pdf_endpoint_refuses_a_channelless_request(monkeypatch):
    """Pre-fix: 200 and a PDF reading "Total Budget $0.00 / Channels 0 / No
    channel data available" -- an empty document dressed as a plan."""
    from routes import export as ex

    sent = []
    monkeypatch.setattr(ex, "_track_export_event", lambda *_a, **_k: None)
    monkeypatch.setattr(ex, "_generate_and_send_pdf", lambda *a, **_k: sent.append(a))

    h = _Handler({"plan_data": WIZARD_REQUEST, "client_name": "Acme Health"})
    ex._handle_pdf_export_post(h, "/api/export/pdf", None)
    assert h.status == 422
    assert "channels" in h.error()
    assert sent == []

    ok = _Handler(
        {
            "plan_data": {"budget": 1000, "channels": [{"name": "LinkedIn", "budget": 1000}]},
            "client_name": "Acme",
        }
    )
    ex._handle_pdf_export_post(ok, "/api/export/pdf", None)
    assert len(sent) == 1


def test_pdf_button_sends_the_csrf_token():
    """Pre-fix: headers were {"Content-Type"} only -> 403 on every click."""
    block = js_block(FOOTER, "function exportPlanPDF()")
    call = block[block.index('fetch("/api/export/pdf"') :]
    call = call[: call.index(".then(")]
    assert '"X-CSRF-Token"' in call, call


# ══════════════════════════════════════════════════════════════════════════
# Client: Excel/CSV availability + request shape (D-07)
# ══════════════════════════════════════════════════════════════════════════

_EXPORT_STUB = r"""
var toasts = [], fetched = [];
var _lastPlanPayload = { client_name: 'Acme Health', industry_label: 'Healthcare', budget_range: '60000' };
var _lastPlanId = __PLAN_ID__;
function showToast(m, t) { toasts.push([m, t]); }
function getCsrfToken() { return 'tok'; }
function fetch(url, opts) { fetched.push({ url: url, body: JSON.parse(opts.body) });
  return new Promise(function () {}); }
var ELS = {
  exportDropdown: { classList: { remove: function () {} } },
  exportBtn: { innerHTML: 'Export', disabled: false },
  exportXlsxBtn: { style: { display: 'none' } },
  exportCsvBtn: { style: { display: 'none' } },
  exportFileDivider: { style: { display: 'none' } },
};
var document = { getElementById: function (id) { return ELS[id] || null; } };
"""


def _export_script(plan_id, calls: str) -> str:
    return "\n".join(
        [
            _EXPORT_STUB.replace("__PLAN_ID__", json.dumps(plan_id)),
            js_block(APP, "function novaPlanFileExportAvailable()"),
            js_block(APP, "function novaSyncExportFileButtons()"),
            js_block(APP, "function exportPlan("),
            calls,
        ]
    )


@needs_node
def test_excel_csv_entries_exist_only_while_a_plan_id_is_known():
    shown = run_node(
        _export_script(
            "b1e515d8ebcf4e28a049a8a686fa1b81",
            "novaSyncExportFileButtons();"
            "process.stdout.write(JSON.stringify(['exportXlsxBtn','exportCsvBtn','exportFileDivider']"
            ".map(function (id) { return ELS[id].style.display; })));",
        )
    )
    assert shown == ["", "", ""]
    hidden = run_node(
        _export_script(
            None,
            "novaSyncExportFileButtons();"
            "process.stdout.write(JSON.stringify(['exportXlsxBtn','exportCsvBtn','exportFileDivider']"
            ".map(function (id) { return ELS[id].style.display; })));",
        )
    )
    assert hidden == ["none", "none", "none"]


@needs_node
def test_file_export_posts_the_plan_id_not_the_wizard_request():
    got = run_node(
        _export_script(
            "b1e515d8ebcf4e28a049a8a686fa1b81",
            "exportPlan('csv'); exportPlan('sheets');"
            "process.stdout.write(JSON.stringify({ fetched: fetched, toasts: toasts }));",
        )
    )
    csv_call, sheets_call = got["fetched"]
    assert csv_call["body"] == {
        "plan_id": "b1e515d8ebcf4e28a049a8a686fa1b81",
        "format": "csv",
        "client_name": "Acme Health",
        "industry_label": "Healthcare",
    }
    assert "plan_data" not in csv_call["body"]
    # Google Sheets keeps its existing contract
    assert sheets_call["body"]["format"] == "sheets"
    assert sheets_call["body"]["plan_data"]["client_name"] == "Acme Health"


@needs_node
def test_file_export_without_a_plan_id_posts_nothing():
    got = run_node(
        _export_script(
            None,
            "exportPlan('xlsx');"
            "process.stdout.write(JSON.stringify({ fetched: fetched, toasts: toasts }));",
        )
    )
    assert got["fetched"] == []
    assert got["toasts"] and got["toasts"][0][1] == "warning"


def test_export_menu_markup_hides_the_file_entries_by_default():
    for element_id in ("exportXlsxBtn", "exportCsvBtn", "exportFileDivider"):
        tag = re.search(r"<(?:button|div)[^>]*\bid=\"%s\"[^>]*>" % element_id, BODY)
        assert tag, element_id
        assert 'style="display: none"' in tag.group(0), tag.group(0)


# ══════════════════════════════════════════════════════════════════════════
# D-13: example chips
# ══════════════════════════════════════════════════════════════════════════

_CHIP_STUB = r"""
var P = __PARAMS__;
var toasts = [], confirmMsgs = [], timers = [];
var locations = P.locations.slice(), roles = P.roles.slice(), selectedIndustry = P.industry;
function confirm(m) { confirmMsgs.push(m); return P.confirmAnswer; }
function showToast(m) { toasts.push(m); }
function renderTags() {}
function addTag(c, i, arr, v) { arr.push(v); }
function selectIndustry(card) { selectedIndustry = card.industry; }
function setTimeout(fn) { fn(); }
var ELS = {};
['clientName', 'clientWebsite', 'useCase', 'exactBudget', 'budgetRange', 'campaignDuration', 'hireVolume'].forEach(function (id) {
  ELS[id] = { value: P[id] || '', dispatchEvent: function () {} };
});
var document = {
  getElementById: function (id) { return ELS[id] || null; },
  querySelector: function (sel) { return /healthcare_medical/.test(sel) ? { industry: 'healthcare_medical' } : null; },
  querySelectorAll: function () { return []; },
};
var chip = { textContent: ' Nurse in LA ', classList: { add: function () {}, remove: function () {} },
  dataset: { client: 'Northwind Health', title: 'Registered Nurse', location: 'Los Angeles, CA',
    industry: 'healthcare_medical', budget: '< $50,000', duration: '3-6 months', hires: '50-100 hires' } };
"""


def _chip_run(**params) -> dict:
    base = {
        "locations": [],
        "roles": [],
        "industry": None,
        "confirmAnswer": True,
        "clientName": "",
        "clientWebsite": "",
        "useCase": "",
        "exactBudget": "",
    }
    base.update(params)
    handler = js_block(APP, 'chip.addEventListener("click", function () {')
    handler = handler[handler.index("function () {") :]
    script = "\n".join(
        [
            _CHIP_STUB.replace("__PARAMS__", json.dumps(base)),
            js_block(APP, "function novaFormHasUserInput()"),
            "var handler = (" + handler + ");",
            "handler.call(chip);",
            "process.stdout.write(JSON.stringify({ confirms: confirmMsgs, toasts: toasts,"
            " client: ELS.clientName.value, website: ELS.clientWebsite.value,"
            " budget: ELS.exactBudget.value,"
            " roles: roles, locations: locations, industry: selectedIndustry }));",
        ]
    )
    return run_node(script)


@needs_node
def test_chip_on_an_empty_form_loads_without_asking():
    got = _chip_run()
    assert got["confirms"] == []
    assert got["client"] == "Northwind Health"
    assert got["roles"] == ["Registered Nurse"]
    assert got["industry"] == "healthcare_medical"


@needs_node
def test_a_typed_website_alone_counts_as_a_filled_form():
    """Round-2 finding: the 'is the form filled?' check ignored the website, so
    an example silently replaced a client whose website was already entered."""
    got = _chip_run(clientWebsite="https://acme.example", confirmAnswer=False)
    assert len(got["confirms"]) == 1
    assert got["website"] == "https://acme.example"
    assert got["client"] == ""
    # confirmed: the website belonged to the replaced client, so it goes too
    got = _chip_run(clientWebsite="https://acme.example", confirmAnswer=True)
    assert got["client"] == "Northwind Health"
    assert got["website"] == ""


@needs_node
def test_chip_over_a_filled_form_asks_and_cancel_changes_nothing():
    got = _chip_run(
        clientName="Acme Welding",
        exactBudget="90,000",
        roles=["Welder"],
        locations=["Detroit, MI"],
        confirmAnswer=False,
    )
    assert len(got["confirms"]) == 1
    assert "Nurse in LA" in got["confirms"][0]
    assert got["client"] == "Acme Welding"
    assert got["budget"] == "90,000"
    assert got["roles"] == ["Welder"]
    assert got["locations"] == ["Detroit, MI"]
    assert got["toasts"] == []


@needs_node
def test_chip_over_a_filled_form_replaces_everything_when_confirmed():
    """Pre-fix: no confirm, and roles became ["Welder", "Registered Nurse"]."""
    got = _chip_run(
        clientName="Acme Welding",
        exactBudget="90,000",
        roles=["Welder"],
        locations=["Detroit, MI"],
        confirmAnswer=True,
    )
    assert len(got["confirms"]) == 1
    assert got["client"] == "Northwind Health"
    assert got["roles"] == ["Registered Nurse"]
    assert got["locations"] == ["Los Angeles, CA"]


# ══════════════════════════════════════════════════════════════════════════
# D-14: form state round trip
# ══════════════════════════════════════════════════════════════════════════

_STATE_STUB = r"""
var P = __PARAMS__;
var store = {};
var sessionStorage = {
  getItem: function (k) { return k in store ? store[k] : null; },
  setItem: function (k, v) { store[k] = String(v); },
  removeItem: function (k) { delete store[k]; },
};
var ELS = {}, BOXES = {}, CARDS = [], insight = 0;
var CHANNEL_IDS = ['chRegionalBoards', 'chGlobalBoards', 'chNicheBoards', 'chSocial',
  'chProgrammatic', 'chEmployerBrand', 'chApac', 'chEmea'];
var SECTION_IDS = ['includeEducational', 'includeEvents', 'includeRadioPodcasts',
  'includeMediaPlatforms', 'includeDEI', 'includeInnovative', 'includeGlobalSupply',
  'includeBudgetGuide'];
function mkEl(id, props) {
  var e = { id: id, value: '', checked: false, tagName: 'INPUT', options: [],
    dispatchEvent: function (ev) {
      if (id === 'regionSelector' && ev.type === 'change') syncRegion();
      return true;
    } };
  for (var k in (props || {})) e[k] = props[k];
  ELS[id] = e;
  return e;
}
// mirrors initRegionSelector's change handler: the region resets APAC/EMEA
function syncRegion() {
  var r = ELS.regionSelector.value;
  ELS.chApac.checked = r === 'apac' || r === 'global' || r === 'custom';
  ELS.chEmea.checked = r === 'emea' || r === 'global' || r === 'custom';
}
['clientName', 'clientWebsite', 'useCase', 'budgetRange', 'budgetPeriod', 'exactBudget',
 'campaignDuration', 'campaignStartMonth', 'hireVolume', 'exactHires', 'targetDemographic',
 'requesterName', 'requesterEmail', 'callTranscript'].forEach(function (id) { mkEl(id); });
mkEl('regionSelector', { value: 'us_only', options: ['us_only', 'global', 'emea', 'apac', 'custom']
  .map(function (v) { return { value: v }; }) });
CHANNEL_IDS.concat(SECTION_IDS).forEach(function (id) { mkEl(id); });
BOXES.customCountry = ['US', 'UK', 'DE'].map(function (v) { return { value: v, checked: false }; });
['campaignGoals', 'workEnvironment', 'experienceLevel'].forEach(function (n) { BOXES[n] = []; });
['professional_white_collar', 'blue_collar'].forEach(function (k) {
  var c = { dataset: { category: k }, selected: false,
    classList: { toggle: function (cls, on) { c.selected = !!on; },
      add: function () {}, remove: function () {} } };
  CARDS.push(c);
});
var document = {
  getElementById: function (id) { return ELS[id] || null; },
  querySelector: function () { return null; },
  querySelectorAll: function (sel) {
    var m = /name="([^"]+)"/.exec(sel || '');
    if (m) {
      var list = BOXES[m[1]] || [];
      return /:checked/.test(sel) ? list.filter(function (c) { return c.checked; }) : list;
    }
    if (/job-cat-card/.test(sel)) return CARDS;
    return [];
  },
};
var window = {};
var channelsDB = { job_categories: {} };
function updateJobCategoryInsight() { insight++; }
var selectedIndustry = null, selectedNaicsCode = null, selectedNaicsTitle = null;
var selectedJobCategories = [], locations = [], roles = [], competitors = [];
"""


def _state_run(**params) -> dict:
    channel_slice = slice_between(
        APP, "const NOVA_CHANNEL_TOGGLES", "function novaChannelCategoriesPayload"
    )
    persist_slice = slice_between(
        APP, 'const _FORM_STORAGE_KEY = "nova_plan_form";', "function clearFormState()"
    )
    body = r"""
function defaults() {
  ELS.regionSelector.value = 'us_only'; syncRegion();
  CHANNEL_IDS.forEach(function (id) { ELS[id].checked = id === 'chSocial'; });
  SECTION_IDS.forEach(function (id) { ELS[id].checked = true; });
  ELS.callTranscript.value = '';
  selectedJobCategories = [];
  CARDS.forEach(function (c) { c.selected = false; });
  BOXES.customCountry.forEach(function (c) { c.checked = false; });
  setNovaChannelsTouched(false);
}
function snapshot() {
  return {
    region: ELS.regionSelector.value,
    custom: BOXES.customCountry.filter(function (c) { return c.checked; }).map(function (c) { return c.value; }),
    apac: ELS.chApac.checked, emea: ELS.chEmea.checked, niche: ELS.chNicheBoards.checked,
    social: ELS.chSocial.checked, edu: ELS.includeEducational.checked,
    events: ELS.includeEvents.checked, transcript: ELS.callTranscript.value,
    jobCats: selectedJobCategories.slice(),
    cardsSelected: CARDS.filter(function (c) { return c.selected; }).map(function (c) { return c.dataset.category; }),
    touched: novaChannelsTouched(),
  };
}
defaults();
ELS.regionSelector.value = P.region;
ELS.regionSelector.dispatchEvent({ type: 'change' });
(P.custom || []).forEach(function (v) {
  BOXES.customCountry.filter(function (c) { return c.value === v; })[0].checked = true;
});
(P.off || []).forEach(function (id) { ELS[id].checked = false; });
(P.on || []).forEach(function (id) { ELS[id].checked = true; });
ELS.includeEducational.checked = false;
ELS.callTranscript.value = 'Need 20 nurses';
selectedJobCategories = ['professional_white_collar'];
CARDS[0].selected = true;   // what clicking the card does
setNovaChannelsTouched(true);
var entered = snapshot();
saveFormState();
var saved = JSON.parse(store[_FORM_STORAGE_KEY]);
defaults();
var restoredFlag = restoreFormState();
var resaved = JSON.parse(store[_FORM_STORAGE_KEY]);
process.stdout.write(JSON.stringify({ entered: entered, restoredFlag: restoredFlag,
  after: snapshot(), saved: { region: saved.region, customCountries: saved.customCountries,
    channels: saved.channels, sections: saved.sections, jobCats: saved.selectedJobCategories,
    transcript: saved.callTranscript }, resavedRegion: resaved.region }));
"""
    script = "\n".join(
        [
            _STATE_STUB.replace("__PARAMS__", json.dumps(params)),
            channel_slice,
            persist_slice,
            body,
        ]
    )
    return run_node(script)


@needs_node
def test_reload_keeps_region_channels_sections_job_categories_and_transcript():
    """Pre-fix: none of these keys were saved and all of them reverted."""
    got = _state_run(region="global", on=["chNicheBoards"])
    assert got["saved"]["region"] == "global"
    assert got["saved"]["jobCats"] == ["professional_white_collar"]
    assert got["saved"]["transcript"] == "Need 20 nurses"
    assert got["saved"]["channels"]["chNicheBoards"] is True
    assert got["saved"]["sections"]["includeEducational"] is False
    assert got["restoredFlag"] is True
    # everything the user set is back, nothing else moved
    assert got["after"] == got["entered"], got
    assert got["after"]["region"] == "global"
    assert got["after"]["cardsSelected"] == ["professional_white_collar"]
    # the restore re-saved the full state for a second reload
    assert got["resavedRegion"] == "global"


@needs_node
def test_saved_channel_toggles_win_over_the_regions_defaults():
    """EMEA ticks the EMEA toggle on change; the user had unticked it."""
    got = _state_run(region="emea", off=["chEmea"])
    assert got["entered"]["emea"] is False
    assert got["after"]["region"] == "emea"
    assert got["after"]["emea"] is False, got["after"]


@needs_node
def test_custom_region_countries_survive_a_reload():
    got = _state_run(region="custom", custom=["US", "DE"])
    assert got["saved"]["customCountries"] == ["US", "DE"]
    assert got["after"]["region"] == "custom"
    assert got["after"]["custom"] == ["US", "DE"]


def test_drafts_carry_the_region_and_use_the_real_job_card_class():
    collect = js_block(APP, "function _collectDraftState()")
    assert "region" in collect and "customCountries" in collect
    # the card class is .job-cat-card; .job-category-card never existed, so a
    # restored draft kept the keys but showed no card selected (a selector
    # string starts with a quote; the explanatory comment does not)
    assert not re.search(r"""["']\.job-category-card""", APP)


_REGIONAL_STUB = r"""
var P = __PARAMS__;
var cards = { apacChannelCard: { style: {} }, emeaChannelCard: { style: {} } };
var locations = P.locations;
var document = { getElementById: function (id) {
  if (id === 'regionSelector') return { value: P.region };
  return cards[id] || null; } };
"""


def _regional(region, locations) -> dict:
    script = "\n".join(
        [
            _REGIONAL_STUB.replace("__PARAMS__", json.dumps({"region": region, "locations": locations})),
            js_block(APP, "function updateRegionalSections()"),
            "updateRegionalSections();",
            "process.stdout.write(JSON.stringify({ apac: cards.apacChannelCard.style.display,"
            " emea: cards.emeaChannelCard.style.display }));",
        ]
    )
    return run_node(script)


@needs_node
def test_regional_cards_follow_the_region_as_well_as_the_locations():
    """Pre-fix: location-only, so a restored Global form hid both cards while
    their toggles stayed ticked."""
    assert _regional("global", ["Detroit, MI"]) == {"apac": "block", "emea": "block"}
    assert _regional("emea", ["Detroit, MI"]) == {"apac": "none", "emea": "block"}
    assert _regional("us_only", ["Detroit, MI"]) == {"apac": "none", "emea": "none"}
    assert _regional("us_only", ["Tokyo, Japan"]) == {"apac": "block", "emea": "none"}


# ══════════════════════════════════════════════════════════════════════════
# D-15 / D-16: layout rules
# ══════════════════════════════════════════════════════════════════════════


def _block_after(css: str, start: int) -> str:
    """Text of the {...} block whose opening brace is the first one at/after
    ``start`` (naive brace count -- CSS strings here carry no braces)."""
    i = css.index("{", start)
    depth = 0
    for k in range(i, len(css)):
        if css[k] == "{":
            depth += 1
        elif css[k] == "}":
            depth -= 1
            if depth == 0:
                return css[i + 1 : k]
    raise ValueError("unbalanced css")


def _media_blocks(css: str, query: str):
    pos = 0
    while True:
        i = css.find(query, pos)
        if i < 0:
            return
        yield _block_after(css, i)
        pos = i + len(query)


def _rule_bodies(css: str, selector: str):
    """Declaration text of every rule whose selector list ends with
    ``selector`` (so ``.np-pane`` also matches ``.foo,\\n.np-pane``)."""
    for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", css):
        selectors = [s.strip() for s in m.group(1).split(",")]
        if selector in selectors:
            yield m.group(2)


def test_preview_pane_is_not_pulled_above_the_form_at_1080():
    blocks = [b for b in _media_blocks(STYLES, "@media (max-width: 1080px)") if ".np-pane" in b]
    assert blocks, "no single-column query found"
    for block in blocks:
        for body in _rule_bodies(block, ".np-pane"):
            assert not re.search(r"\border\s*:", body), body
        assert "order: -1" not in block


def test_button_rows_wrap_and_keep_labels_whole():
    wraps = [b for b in _rule_bodies(STYLES, ".studio .btn-row") if "flex-wrap: wrap" in b]
    assert wraps, ".studio .btn-row has no wrap rule"
    nowrap = [
        b
        for b in _rule_bodies(STYLES, ".studio .btn-row > .btn")
        if "white-space: nowrap" in b and "flex: 0 0 auto" in b
    ]
    assert nowrap


def test_generate_button_is_not_pinned_to_width_100_inline():
    tag = re.search(r"<button[^>]*\bid=\"generateBtn\"[^>]*>", BODY, re.S)
    assert tag
    assert "width" not in tag.group(0), tag.group(0)


def test_output_format_toggle_stacks_on_phones():
    phone = [b for b in _media_blocks(STYLES, "@media (max-width: 600px)") if ".output-format-toggle" in b]
    assert phone, "output-format toggle has no phone rule"
    assert any("flex-direction: column" in body for b in phone for body in _rule_bodies(b, ".output-format-toggle"))


# ══════════════════════════════════════════════════════════════════════════
# D-24 accessibility
# ══════════════════════════════════════════════════════════════════════════

_TAGS_STUB = r"""
var removed = [];
function mk(tag) {
  var e = { tagName: tag.toUpperCase(), className: '', children: [], attrs: {}, textContent: '', type: '',
    appendChild: function (c) { this.children.push(c); return c; },
    setAttribute: function (k, v) { this.attrs[k] = v; }, focus: function () {} };
  Object.defineProperty(e, 'innerHTML', { set: function () { this.children = []; } });
  return e;
}
var BOX = mk('div'), INPUT = mk('input');
var document = { getElementById: function (id) { return id === 'box' ? BOX : INPUT; },
  createElement: mk };
function removeTag(c, i, arr, idx) { removed.push(idx); }
function getArrayByContainerId() { return ['Welder']; }
"""


@needs_node
def test_tag_remove_is_a_labelled_button():
    """Pre-fix: a bare <span> with no label, unreachable by keyboard."""
    script = "\n".join(
        [
            _TAGS_STUB,
            js_block(APP, "function renderTags("),
            "renderTags('box', 'input', ['Welder', 'Pipefitter']);",
            "var tag = BOX.children[1];",
            "var rm = tag.children[1];",
            "rm.onclick();",
            "process.stdout.write(JSON.stringify({ tag: rm.tagName, type: rm.type, cls: rm.className,"
            " label: rm.attrs['aria-label'], removed: removed, inputLast: BOX.children[BOX.children.length - 1] === INPUT }));",
        ]
    )
    got = run_node(script)
    assert got["tag"] == "BUTTON"
    assert got["type"] == "button"
    assert got["cls"] == "remove"
    assert got["label"] == "Remove Pipefitter"
    assert got["removed"] == [1]
    assert got["inputLast"] is True


def test_alertdialogs_are_modal_and_focusable():
    for element_id in ("generationOverlay", "qaGateOverlay"):
        tag = re.search(r"<div[^>]*\bid=\"%s\"[^>]*>" % element_id, BODY, re.S)
        assert tag, element_id
        text = tag.group(0)
        assert 'role="alertdialog"' in text
        assert 'aria-modal="true"' in text, text
        assert 'tabindex="-1"' in text, text


@needs_node
def test_dialog_focus_moves_in_and_returns():
    script = "\n".join(
        [
            "var log = [];",
            "var btn = { id: 'generateBtn', focus: function () { log.push('btn'); } };",
            "var overlay = { focus: function () { log.push('overlay'); } };",
            "var document = { activeElement: btn, body: {}, contains: function () { return true; } };",
            "var _novaDialogReturnFocus = null;",
            js_block(APP, "function novaDialogFocusIn("),
            js_block(APP, "function novaDialogFocusOut("),
            "novaDialogFocusIn(overlay); document.activeElement = overlay; novaDialogFocusOut();",
            "process.stdout.write(JSON.stringify(log));",
        ]
    )
    assert run_node(script) == ["overlay", "btn"]


def test_every_overlay_open_and_close_moves_focus():
    assert "novaDialogFocusIn(overlay)" in js_block(APP, "function showGenerationOverlay()")
    assert "novaDialogFocusIn(overlay)" in js_block(APP, "function showQaGateOverlay(")
    assert "novaDialogFocusOut()" in js_block(APP, "function hideGenerationOverlay(")
    assert "novaDialogFocusOut()" in js_block(APP, "function hideQaGateOverlay()")


def test_index_has_exactly_one_h1_the_studio_title():
    count = sum(
        len(re.findall(r"<h1\b", p.read_text(encoding="utf-8")))
        for p in PARTIALS.glob("*.html")
        if p.name != "head_styles.html"  # its comments name the tag
    )
    assert count == 1
    assert '<h1 class="stb-title">Nova <b>Plan Studio</b></h1>' in BODY
    title_rules = list(_rule_bodies(STYLES, ".studio-topbar .stb-title"))
    assert any(re.search(r"margin\s*:\s*0\b", b) for b in title_rules)


# ══════════════════════════════════════════════════════════════════════════
# Copy: the past-campaign upload is reference only
# ══════════════════════════════════════════════════════════════════════════


def test_past_campaign_upload_does_not_claim_to_calibrate_the_plan():
    for phrase in (
        "We'll analyze what worked",
        "What\n          You Did vs. What We Recommend",
        "You Did vs. What We Recommend",
        "sharpen the plan",
        "Upload historical campaign performance data",
    ):
        assert phrase not in BODY, phrase
    zone = BODY[BODY.index('id="historicalUploadZone"') :]
    zone = zone[: zone.index('id="historicalFileList"')]
    assert "reference only" in zone.lower()
    help_text = BODY[BODY.index('id="historicalFileList"') :]
    help_text = " ".join(help_text[: help_text.index("</div>", help_text.index("form-hint"))].split())
    assert "does not calibrate the plan" in help_text, help_text


# ══════════════════════════════════════════════════════════════════════════
# Round 2 (design-judge, mechanism + typography + trust & proof)
# ══════════════════════════════════════════════════════════════════════════

_ALL_COPY = BODY + APP + FOOTER + PREVIEW + META


def test_nothing_on_the_page_claims_a_live_check_that_does_not_happen():
    """Trust lens VETO. The 91 was a static file of 90 profiles (2026-03-22),
    the estimate checks no live boards, generation calls a few live APIs."""
    for claim in (
        "91+",
        "live job boards",
        "Querying 15 live APIs",
        "up to 15 live APIs",
        "25 real-time",
        "25+ APIs",
        "in real time",
    ):
        assert claim not in _ALL_COPY, claim
    assert "90 platform profiles" in " ".join(BODY.split())  # wraps across lines
    assert "Nova is calibrating" not in BODY


def test_the_agent_strip_is_one_honest_line_not_a_ticking_checklist():
    strip = BODY[BODY.index('id="npAgentStrip"') :]
    strip = strip[: strip.index("</div>\n  </div>") + len("</div>\n  </div>")]
    assert "Updating estimate" in strip
    assert "agent-line" not in strip and "&#10003;" not in strip
    assert "agent-line" not in STYLES
    # the strip follows the REAL estimate request, not a fixed timer
    fetch = js_block(PREVIEW, "function fetchEstimate(")
    assert "setEstimating(true)" in fetch
    assert fetch.count("setEstimating(false)") >= 3  # no-input, answered, failed
    assert "pane.__rc" not in PREVIEW and "650" not in js_block(PREVIEW, "function render()")


def test_plan_ready_is_gated_on_a_generated_plan():
    ready_rules = [
        b
        for sel in (".np-pane.plan-generated .np-card:not(.is-empty) .np-ready",)
        for b in _rule_bodies(STYLES, sel)
    ]
    assert ready_rules and "display: inline-flex" in ready_rules[0]
    # no unconditional "has a budget -> Plan ready" rule is left
    assert not list(_rule_bodies(STYLES, ".np-card:not(.is-empty) .np-ready"))
    assert "markGenerated" in PREVIEW and "NovaPreview.markGenerated" in APP
    assert "plan-generated" in js_block(PREVIEW, "function render()")


def test_example_chips_use_a_fictional_client():
    assert "CareFirst" not in BODY
    assert 'data-client="Northwind Health"' in BODY


_FMT_STUB = r"""
var latestEstimate = null;
function novaPlanCurrency() { return null; }
"""


@needs_node
def test_directional_cost_per_application_is_whole_currency_units():
    script = "\n".join(
        [
            _FMT_STUB,
            js_block(PREVIEW, "function sym()"),
            js_block(PREVIEW, "function cpaFmt("),
            "process.stdout.write(JSON.stringify([17.36, 87.54, 9.5, 1.4, 0.62, 0, null]"
            ".map(cpaFmt)));",
        ]
    )
    assert run_node(script) == ["$17", "$88", "$10", "$1", "$0.62", "—", "—"]


def test_pdf_action_is_honest_about_what_it_does():
    """Trust lens B: no plan id means no server PDF, so the button prints."""
    assert "Print / Save as PDF" in BODY
    assert "Export PDF" not in BODY and "Download PDF Report" not in BODY
    block = js_block(FOOTER, "function exportPlanPDF()")
    assert "novaPrintPlan()" in block
    assert "this request has none" not in FOOTER


_PDF_STUB = r"""
var prints = 0, toasts = [], fetched = [];
var window = { print: function () { prints++; } };
function showToast(m, t) { toasts.push([m, t]); }
function getCsrfToken() { return 'tok'; }
function setTimeout(fn) { fn(); }
var _lastPlanPayload = { client_name: 'Acme', industry: 'healthcare_medical' };
var planIdKnown = __PLAN_ID_KNOWN__;
function novaPlanFileExportAvailable() { return planIdKnown; }
var ELS = { exportDropdown: { classList: { remove: function () {} } },
  exportBtn: { innerHTML: 'Export', disabled: false } };
var document = { getElementById: function (id) { return ELS[id] || null; },
  createElement: function () { return { click: function () {}, remove: function () {} }; },
  body: { appendChild: function () {} } };
function fetch(url, opts) {
  fetched.push({ url: url, token: (opts.headers || {})['X-CSRF-Token'] });
  return Promise.resolve({ ok: false, json: function () { return Promise.resolve({ error: 'no channels' }); } });
}
"""


def _pdf_script(plan_id_known: bool) -> str:
    return "\n".join(
        [
            _PDF_STUB.replace("__PLAN_ID_KNOWN__", "true" if plan_id_known else "false"),
            js_block(FOOTER, "function novaPrintPlan()"),
            js_block(FOOTER, "function exportPlanPDF()"),
            "exportPlanPDF();",
            "setImmediate(function () { setImmediate(function () { setImmediate(function () {",
            "  process.stdout.write(JSON.stringify({ prints: prints, toasts: toasts, fetched: fetched }));",
            "}); }); });",
        ]
    )


@needs_node
def test_pdf_button_without_a_plan_id_prints_at_once_with_no_toast():
    got = run_node(_pdf_script(False))
    assert got == {"prints": 1, "toasts": [], "fetched": []}


@needs_node
def test_pdf_button_with_a_plan_id_asks_the_server_with_the_token_then_prints_quietly():
    got = run_node(_pdf_script(True))
    assert got["fetched"] == [{"url": "/api/export/pdf", "token": "tok"}]
    assert got["prints"] == 1
    assert got["toasts"] == []  # a failed server PDF is not an error to show


def test_one_truthful_generation_time_everywhere():
    assert "Takes about a minute" in BODY
    assert "Generation time: <strong>about a minute</strong>" in BODY
    for stale in ("&lt;30 seconds", "30-90 seconds", "under 30 seconds"):
        assert stale not in BODY, stale


# ── sticky estimate bar (mechanism) ──────────────────────────────────────


def test_estimate_bar_markup_and_css_are_single_column_only():
    tag = re.search(r"<button[^>]*\bid=\"npEstimateBar\"[^>]*>", BODY, re.S)
    assert tag, "no #npEstimateBar"
    text = tag.group(0)
    assert "bare-btn" in text  # opts out of the global button rule that forces position: relative
    assert 'aria-controls="planPreview"' in text
    for fig in ("nebBudget", "nebApps", "nebHires", "nebCpa"):
        assert f'id="{fig}"' in BODY
    base = [b for b in _rule_bodies(STYLES, ".np-estimate-bar") if "display: none" in b]
    assert base, "the bar must be display:none outside the single-column query"
    shown = [
        b
        for blk in _media_blocks(STYLES, "@media (max-width: 1080px)")
        for b in _rule_bodies(blk, ".np-estimate-bar")
        if "position: sticky" in b
    ]
    assert shown and "display: flex" in shown[0] and "top: 58px" in shown[0]
    # the desktop two-column layout never renders it
    assert not [
        blk
        for blk in _media_blocks(STYLES, "@media (min-width: 1081px)")
        if ".np-estimate-bar" in blk
    ]


_BAR_STUB = r"""
var latestEstimate = __EST__;
var attrs = {}, texts = {};
var bar = { setAttribute: function (k, v) { attrs[k] = v; } };
function $(id) {
  if (id === 'npEstimateBar') return bar;
  texts[id] = texts[id] || { textContent: '' };
  return texts[id];
}
function hero(v) { return '$' + Math.round(v / 1000) + 'K'; }
function intc(v) { return Math.round(v).toLocaleString('en-US'); }
function cpaFmt(v) { return '$' + Math.round(v); }
"""


def _bar_run(estimate, total) -> dict:
    script = "\n".join(
        [
            _BAR_STUB.replace("__EST__", json.dumps(estimate)),
            js_block(PREVIEW, "function paintEstimateBar("),
            f"paintEstimateBar({{ total: {total} }});",
            "var figs = {}; Object.keys(texts).forEach(function (k) { figs[k] = texts[k].textContent; });",
            "process.stdout.write(JSON.stringify({ attrs: attrs, figs: figs }));",
        ]
    )
    return run_node(script)


@needs_node
def test_estimate_bar_shows_the_four_figures_once_an_estimate_exists():
    got = _bar_run({"est_applications": 2016.4, "est_hires": 6.2, "est_cpa": 17.36}, 35000)
    assert got["attrs"]["data-state"] == "ready"
    assert got["figs"] == {
        "nebBudget": "$35K",
        "nebApps": "2,016",
        "nebHires": "6",
        "nebCpa": "$17",
    }
    label = got["attrs"]["aria-label"]
    assert "$35K budget" in label and "2,016 applications" in label and "per application" in label


@needs_node
def test_estimate_bar_is_a_hint_until_the_estimate_arrives():
    for estimate, total in ((None, 35000), ({"est_applications": 1, "est_hires": 1, "est_cpa": 1}, 0)):
        got = _bar_run(estimate, total)
        assert got["attrs"]["data-state"] == "empty"
        assert got["figs"] == {}
        assert "set a budget" in got["attrs"]["aria-label"].lower()


def test_estimate_bar_tap_scrolls_with_the_reduced_motion_aware_behaviour():
    block = js_block(PREVIEW, "function showPane(")
    assert "novaScrollBehavior()" in block  # "auto" under prefers-reduced-motion
    assert "scrollIntoView" in block
    # the pane lands below the sticky top bar + estimate bar
    pane = [
        b
        for blk in _media_blocks(STYLES, "@media (max-width: 1080px)")
        for b in _rule_bodies(blk, ".np-pane")
    ]
    assert any("scroll-margin-top" in b and "position: relative" in b for b in pane)
    reduced = "\n".join(_media_blocks(STYLES, "@media (prefers-reduced-motion: reduce)"))
    assert ".np-estimate-bar" in reduced


# ── typography / contrast ─────────────────────────────────────────────────


def _luminance(hex_color: str) -> float:
    rgb = [int(hex_color.lstrip("#")[i : i + 2], 16) / 255 for i in (0, 2, 4)]
    lin = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]


def _contrast(a: str, b: str) -> float:
    hi, lo = sorted((_luminance(a), _luminance(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def test_small_form_labels_clear_aa_on_the_brightest_part_of_the_dark_column():
    """Measured from the 375px render: the aurora's brightest corner, behind
    the first step labels, is rgb(44,48,88). --np-faint (#9794b6) was 4.3:1
    there (and 1.1-1.3:1 on the lavender wash the single-column layout used
    to paint over the whole page)."""
    brightest = "#2c3058"
    muted = re.search(r"--np-muted:\s*(#[0-9a-fA-F]{6})", STYLES)
    assert muted
    assert _contrast(muted.group(1), brightest) >= 4.5
    for selector in (".studio .step-label", ".studio .preset-label"):
        body = next(
            b for b in _rule_bodies(STYLES, selector) if re.search(r"\bcolor\s*:", b)
        )
        assert "var(--np-muted)" in body, (selector, body)


def test_the_lavender_wash_stays_inside_the_preview_pane():
    """`.np-pane::before` is position:absolute; on a static pane its
    containing block is .studio, so the wash covered the whole form column."""
    panes = [
        b
        for blk in _media_blocks(STYLES, "@media (max-width: 1080px)")
        for b in _rule_bodies(blk, ".np-pane")
    ]
    assert panes and all("position: static" not in b for b in panes)
    assert any("position: relative" in b for b in panes)


# ── measured defects ──────────────────────────────────────────────────────


def test_tag_remove_hit_area_is_at_least_24px_and_the_glyph_unchanged():
    body = next(b for b in _rule_bodies(STYLES, ".tag .remove") if "min-width" in b)
    assert "min-width: 24px" in body and "min-height: 24px" in body
    assert "font-size: 14px" in body  # glyph unchanged
    assert re.search(r"margin:\s*-3px -5px -3px -4px", body)  # tag keeps its size
    opacity = float(re.search(r"opacity:\s*([0-9.]+)", body).group(1))  # type: ignore[union-attr]
    assert opacity >= 0.7


def test_fixed_overlays_step_aside_while_a_field_has_focus_on_small_screens():
    blocks = [
        blk
        for blk in _media_blocks(STYLES, "@media (max-width: 1080px)")
        if "nova-field-focus" in blk
    ]
    assert blocks
    text = " ".join(blocks)
    assert "body.nova-field-focus #back-to-suite" in text
    assert "body.nova-field-focus #nova-float-btn:not(.nova-btn-close)" in text
    assert "opacity: 0" in text and "pointer-events: none" in text
    assert "nova-field-focus" in A11Y and "focusin" in A11Y and "focusout" in A11Y
