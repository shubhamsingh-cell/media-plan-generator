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
var _regionUserPicked = false;  // saveFormState / novaRestoreRegion carry the flag
var excludedPublishers = [];  // saveFormState / restoreFormState carry the excluded boards
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
var latestEstimate = null, latestEstimateSig = '', estimating = false, lastScheduledSig = '';
var attrs = {}, texts = {};
function estimatePayload(m) { return m.inputs; }
function inputsFor(total, roles) { return { target_roles: roles, budget_range: total }; }
// an estimate computed for exactly these inputs
function ready(est, total, roles) { latestEstimate = est; latestEstimateSig = JSON.stringify(inputsFor(total, roles)); }
// a request for exactly these inputs was scheduled (and may have failed)
function scheduled(total, roles) { lastScheduledSig = JSON.stringify(inputsFor(total, roles)); }
var bar = { setAttribute: function (k, v) { attrs[k] = v; } };
function $(id) {
  if (id === 'npEstimateBar') return bar;
  texts[id] = texts[id] || { textContent: '' };
  return texts[id];
}
function hero(v) { return '$' + Math.round(v / 1000) + 'K'; }
function intc(v) { return Math.round(v).toLocaleString('en-US'); }
function appsFmt(v) { return (v >= 100 ? Math.round(v / 10) * 10 : Math.round(v)).toLocaleString('en-US'); }
function cpaFmt(v) { return '$' + Math.round(v); }
var document = { documentElement: { style: { setProperty: function (k, v) { attrs[k] = v; }, removeProperty: function (k) { delete attrs[k]; } } } };
function sym() { return '$'; }
function trim(x, d) { return Number(x.toFixed(d)).toString(); }
function figs() { var o = {}; Object.keys(texts).forEach(function (k) { if (k !== 'nebBasis') o[k] = texts[k].textContent; }); return o; }
function paint(total, roles) { paintEstimateBar({ total: total, inputs: inputsFor(total, roles) }); }
"""

_EST = {"est_applications": 2016.4, "est_hires": 6.2, "est_cpa": 17.36}


def _bar_script(body: str) -> dict:
    return run_node(
        "\n".join(
            [
                _BAR_STUB,
                js_block(PREVIEW, "function estimateFor("),
                js_block(PREVIEW, "function hireBasisText("),
                js_block(PREVIEW, "function syncEstimateBarHeight("),
                js_block(PREVIEW, "function paintEstimateBar("),
                body,
                "process.stdout.write(JSON.stringify(out));",
            ]
        )
    )


@needs_node
def test_estimate_bar_shows_the_four_figures_once_an_estimate_exists():
    got = _bar_script(
        "ready(" + json.dumps(_EST) + ", 35000, ['Nurse']); paint(35000, ['Nurse']);"
        "var out = { attrs: attrs, figs: figs() };"
    )
    assert got["attrs"]["data-state"] == "ready"
    assert got["attrs"]["aria-busy"] == "false"
    assert got["figs"] == {
        "nebBudget": "$35K",
        "nebApps": "2,020",  # directional: to the nearest 10 from 100 up
        "nebHires": "6",
        "nebCpa": "$17",
    }
    label = got["attrs"]["aria-label"]
    assert "$35K budget" in label and "2,020 applications" in label and "per application" in label


@needs_node
def test_estimate_bar_hint_is_only_for_genuinely_missing_inputs():
    """Round-3: with a budget AND a role set the hint used to come back on
    every edit (fetchEstimate clears the estimate while the request runs)."""
    for total, roles in ((0, ["Nurse"]), (35000, [])):
        got = _bar_script(f"paint({total}, {json.dumps(roles)}); var out = {{ attrs: attrs, figs: figs() }};")
        assert got["attrs"]["data-state"] == "empty"
        assert got["figs"] == {}
        assert "set a budget" in got["attrs"]["aria-label"].lower()
    # inputs set, estimate pending: NOT the hint
    got = _bar_script("estimating = true; paint(35000, ['Nurse']); var out = { attrs: attrs };")
    assert got["attrs"]["data-state"] == "loading"


@needs_node
def test_estimate_bar_never_pairs_a_new_budget_with_old_figures():
    """Round-4: a budget edit left the previous estimate's figures on the bar
    (and, while the retries ran, dashes flickered in between). Loading now
    shows the CURRENT budget with "…" where the figures go, and no earlier
    figure appears anywhere on the bar."""
    got = _bar_script(
        "ready(" + json.dumps(_EST) + ", 35000, ['Nurse']); paint(35000, ['Nurse']);"
        # the budget is edited: the estimate in hand answers 35000, the inputs say 40000
        "estimating = true; paint(40000, ['Nurse']);"
        "var out = { attrs: attrs, figs: figs() };"
    )
    assert got["attrs"]["data-state"] == "loading"
    assert got["attrs"]["aria-busy"] == "true"
    assert got["figs"] == {"nebBudget": "$40K", "nebApps": "…", "nebHires": "…", "nebCpa": "…"}
    assert "updating" in got["attrs"]["aria-label"].lower()
    for old_figure in ("2,020", "$17", "$35K"):
        assert old_figure not in json.dumps(got["figs"])
    # first estimate ever, still pending: the same placeholders under the same status
    got = _bar_script("estimating = true; paint(40000, ['Nurse']); var out = { attrs: attrs, figs: figs() };")
    assert got["figs"] == {"nebBudget": "$40K", "nebApps": "…", "nebHires": "…", "nebCpa": "…"}


@needs_node
def test_estimate_bar_ignores_an_estimate_computed_for_other_inputs():
    """Even when nothing is in flight yet (the budget field fires its handlers
    before render() schedules the request) the old estimate is not "ready" for
    the new budget."""
    got = _bar_script(
        "ready(" + json.dumps(_EST) + ", 35000, ['Nurse']); scheduled(35000, ['Nurse']);"
        "estimating = false; paint(40000, ['Nurse']);"
        "var out = { attrs: attrs, figs: figs() };"
    )
    # no request exists for 40000 yet: it is about to be scheduled, so "updating"
    assert got["attrs"]["data-state"] == "loading"
    assert "2,020" not in json.dumps(got["figs"])
    # a different ROLE is different inputs too
    got = _bar_script(
        "ready(" + json.dumps(_EST) + ", 35000, ['Nurse']); estimating = true; paint(35000, ['Welder']);"
        "var out = { attrs: attrs };"
    )
    assert got["attrs"]["data-state"] == "loading"


@needs_node
def test_estimate_bar_shows_dashes_when_the_request_failed_like_the_pane():
    got = _bar_script(
        "ready(" + json.dumps(_EST) + ", 35000, ['Nurse']); paint(35000, ['Nurse']);"
        # failure: no estimate, nothing in flight; the stale figures must NOT come back
        "latestEstimate = null; latestEstimateSig = ''; estimating = false; scheduled(35000, ['Nurse']);"
        "paint(35000, ['Nurse']);"
        "var out = { attrs: attrs, figs: figs() };"
    )
    assert got["attrs"]["data-state"] == "unavailable"
    assert got["figs"] == {"nebBudget": "$35K", "nebApps": "—", "nebHires": "—", "nebCpa": "—"}
    assert "unavailable" in got["attrs"]["aria-label"].lower()


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


# ══════════════════════════════════════════════════════════════════════════
# Round 3 (re-judge: trust veto on head claims, feasibility banner, layout,
# toasts, estimate-bar states)
# ══════════════════════════════════════════════════════════════════════════

# every number the head/meta/JSON-LD may still carry, and where it comes from:
#   90        data/platform_intelligence_deep.json  "platforms": 90
#   11        excel_v2.py: at most 11 worksheets (ws .. ws11)
#   15, 35    data/external_benchmarks_2025.json    cpa range_excl_healthcare "$15-$35"
#   2024/2025 Appcast 2025 Recruitment Marketing Benchmark Report (2024 data)
#   0         schema.org Offer "price": "0" -- the tool is free (no paywall)
_TRACEABLE_HEAD_FIGURES = {"90", "11", "15", "35", "2024", "2025", "0"}


def _head_strings() -> list:
    out = [
        m.group(1)
        for m in re.finditer(
            r'<meta[^>]*?(?:name|property)="(?:description|og:description|twitter:description)"[^>]*?content="([^"]*)"',
            META,
            re.S,
        )
    ]
    for block in re.findall(r'<script type="application/ld\+json">(.*?)</script>', META, re.S):
        stack = [json.loads(block)]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
            elif isinstance(node, str) and not node.startswith("http"):
                out.append(node)
    return out


def test_every_figure_in_the_head_traces_to_a_data_file_or_is_gone():
    strings = _head_strings()
    assert len(strings) > 15
    figures = {n for s in strings for n in re.findall(r"\d+", s)}
    assert figures <= _TRACEABLE_HEAD_FIGURES, sorted(figures - _TRACEABLE_HEAD_FIGURES)
    joined = " ".join(strings)
    for gone in ("$15-45", "40-60%", "12+", "20-40%", "30-50%", "PT2M"):
        assert gone not in joined and gone not in META, gone
    assert "up to 11 tabs" in joined
    assert "$15-$35 outside healthcare" in joined
    assert "totalTime" not in META  # the "2 minutes" had no source either


def test_loading_screen_facts_carry_no_untraceable_number():
    facts = js_block(APP, "const GEN_FACTS")
    assert "up to 12 sources" not in facts
    assert "379M clicks" in facts and "Appcast" in facts  # sourced figure stays, attributed


def test_nothing_says_real_campaign_calibration_is_on():
    """Calibration on Joveo campaign results is OFF by default."""
    gate = (PARTIALS.parent.parent.parent / "static" / "nova-auth-gate.js").read_text(encoding="utf-8")
    for text in (BODY, gate):
        assert "real campaign data" not in text
        assert "real Joveo campaign" not in text
    assert "Based on Joveo channel benchmarks" in BODY
    assert "based on Joveo channel benchmarks." in gate


_FEAS_STUB = r"""
var P = __PARAMS__;
var existing = null, inserted = null;
var hireEl = { closest: function () { return null; }, parentElement: { closest: function () { return null; },
  parentElement: { insertBefore: function (el) { inserted = el; existing = el; } }, nextSibling: null } };
var document = { getElementById: function (id) {
  if (id === 'hireVolume') return hireEl;
  if (id === 'budgetFeasibilityWarning') return existing;
  return null; },
  createElement: function () { var e = { id: '', style: {}, innerHTML: '', attrs: {},
    setAttribute: function (k, v) { this.attrs[k] = v; }, remove: function () { existing = null; } }; return e; } };
function escapeHtml(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;'); }
var window = { NovaPreview: { feasibility: function () { return P.f; } } };
"""


def _feas(f, pre_existing=False) -> dict:
    script = "\n".join(
        [
            _FEAS_STUB.replace("__PARAMS__", json.dumps({"f": f})),
            (
                "existing = { innerHTML: 'old', remove: function () { existing = null; } };"
                if pre_existing
                else ""
            ),
            js_block(APP, "function checkBudgetFeasibility("),
            "checkBudgetFeasibility();",
            "process.stdout.write(JSON.stringify({ html: existing ? existing.innerHTML : null }));",
        ]
    )
    return run_node(script)


_FEAS_FIGURES = {
    "ratio": 6.2 / 75, "budget": "$35K", "hires": "6", "goal": "75-hire target", "cph": "$6K", "gap": "$403K",
}


@needs_node
def test_budget_reality_check_words_the_estimate_engines_figures_only():
    """Round-3: it printed "A __exact__ budget for ~75 hires equals ~$640/hire
    ... could support ~10 hire(s) ... need ~$262,500+" beside a pane saying
    ~6 hires and ~$403K more."""
    html = _feas(_FEAS_FIGURES)["html"]
    assert html is not None
    assert "__exact__" not in html
    assert "~6 hires" in html and "~$6K/hire" in html and "75-hire target" in html
    assert "$403K" in html and "$35K" in html
    assert "UNREALISTIC" in html  # ratio 0.08 < 0.1
    assert "$262,500" not in html


@needs_node
def test_budget_reality_check_says_nothing_without_an_estimate_or_when_funded():
    assert _feas(None)["html"] is None
    assert _feas(dict(_FEAS_FIGURES, ratio=0.5))["html"] is None
    # a banner that is already up is removed once the estimate says it is fine
    assert _feas(dict(_FEAS_FIGURES, ratio=0.9), pre_existing=True)["html"] is None
    assert _feas(None, pre_existing=True)["html"] is None


def test_the_old_feasibility_arithmetic_is_gone():
    for dead in (
        "INDUSTRY_CPH_BENCHMARKS",
        "BUDGET_MIDPOINTS",
        "HIRE_VOLUME_MAP",
        "getIndustryCphKey",
    ):
        assert dead not in APP, dead
    block = js_block(APP, "function checkBudgetFeasibility(")
    assert "budgetStr" not in block and "NovaPreview.feasibility" in block
    assert "feasibility: feasibility" in PREVIEW


_FEAS_ENGINE_STUB = r"""
var latestEstimate = __EST__;
// the estimate was computed for budget __SIGBUDGET__; the inputs say __TOTAL__
var latestEstimateSig = __EST__ ? JSON.stringify({ budget: __SIGBUDGET__ }) : '';
function gather() { return { total: __TOTAL__, targetHires: __TARGET__, goalRange: '__RANGE__', inputs: { budget: __TOTAL__ } }; }
function estimatePayload(m) { return m.inputs; }
function hero(v) { return '$' + Math.round(v / 1000) + 'K'; }
function intc(v) { return Math.round(v).toLocaleString('en-US'); }
function sym() { return '$'; }
function trim(x, d) { return Number(x.toFixed(d)).toString(); }
"""


def _engine_feasibility(est, total, target, goal_range="", estimate_for_budget=None):
    script = "\n".join(
        [
            _FEAS_ENGINE_STUB.replace("__EST__", json.dumps(est))
            .replace("__SIGBUDGET__", str(total if estimate_for_budget is None else estimate_for_budget))
            .replace("__TOTAL__", str(total))
            .replace("__TARGET__", str(target))
            .replace("__RANGE__", goal_range),
            js_block(PREVIEW, "function estimateFor("),
            js_block(PREVIEW, "function goalText("),
            js_block(PREVIEW, "function hireBasisText("),
            js_block(PREVIEW, "function budgetGap("),
            js_block(PREVIEW, "function feasibility("),
            "process.stdout.write(JSON.stringify(feasibility()));",
        ]
    )
    return run_node(script)


@needs_node
def test_feasibility_uses_the_same_numbers_as_the_nova_insight():
    est = {"est_hires": 6.0, "est_cph": 5645}
    got = _engine_feasibility(est, 35000, 75)
    # insight #1 in computeInsights: gap = total * (target / est_hires - 1)
    assert got["gap"] == "$403K" and got["hires"] == "6" and got["goal"] == "75-hire target"
    assert got["cph"] == "$6K" and got["budget"] == "$35K"
    assert round(got["ratio"], 3) == round(6.0 / 75, 3)
    assert _engine_feasibility(None, 35000, 75) is None
    assert _engine_feasibility(est, 0, 75) is None
    assert _engine_feasibility(est, 35000, 0) is None


@needs_node
def test_feasibility_is_null_for_an_estimate_computed_for_a_different_budget():
    """Round-4: typing a new budget ran the Budget Reality Check (the field's
    own input handler) against the PREVIOUS budget's hires, so "At $60K the
    estimate supports ~6 hires" came from the $35K estimate."""
    est = {"est_hires": 6.0, "est_cph": 5645}
    assert _engine_feasibility(est, 60000, 75, estimate_for_budget=35000) is None
    assert _engine_feasibility(est, 60000, 75, estimate_for_budget=60000) is not None


@needs_node
def test_a_picked_range_is_worded_as_the_range_not_one_number():
    """"50-100 hires" is planned at ~75; the copy must say so rather than
    restate it as "your 75-hire target" (a number the user never chose)."""
    est = {"est_hires": 6.0, "est_cph": 5645}
    ranged = _engine_feasibility(est, 35000, 75, goal_range="50\u2013100")
    assert ranged["goal"] == "50\u2013100 hire goal (planned at ~75)"
    exact = _engine_feasibility(est, 35000, 75)
    assert exact["goal"] == "75-hire target"


@needs_node
def test_applications_round_to_the_nearest_ten_from_one_hundred():
    script = "\n".join(
        [
            js_block(PREVIEW, "function appsFmt("),
            "process.stdout.write(JSON.stringify([2016.4, 2014, 104, 100, 99.6, 42.4, 0, -3].map(appsFmt)));",
        ]
    )
    assert run_node(script) == ["2,020", "2,010", "100", "100", "100", "42", "0", "0"]


# ── layout (round 3) ──────────────────────────────────────────────────────


def test_form_card_side_padding_shrinks_on_phones():
    phone = [
        b
        for blk in _media_blocks(STYLES, "@media (max-width: 480px)")
        for b in _rule_bodies(blk, ".studio .card")
    ]
    assert any(re.search(r"padding:\s*24px 18px !important", b) for b in phone), phone


def test_sticky_bar_gutter_label_size_and_states():
    bar = [
        b
        for blk in _media_blocks(STYLES, "@media (max-width: 1080px)")
        for b in _rule_bodies(blk, ".np-estimate-bar")
        if "position: sticky" in b
    ]
    assert bar and "clamp(16px, 4vw, 40px)" in bar[0]  # the form's own gutter: 16px at 375
    label = [
        b
        for blk in _media_blocks(STYLES, "@media (max-width: 1080px)")
        for b in _rule_bodies(blk, ".np-estimate-bar .neb-item span")
    ]
    assert label and float(re.search(r"font-size:\s*([0-9.]+)px", label[0]).group(1)) >= 11  # type: ignore[union-attr]
    css = "\n".join(_media_blocks(STYLES, "@media (max-width: 1080px)"))
    assert '.np-estimate-bar[data-state="loading"] .neb-status' in css
    assert '.np-estimate-bar[data-state="loading"] .neb-item b' in css
    for state in ("loading", "empty"):
        assert f'data-state="{state}"' in BODY or state == "loading"
    assert 'data-state="empty"' in BODY and "neb-status" in BODY


def test_floating_controls_leave_the_form_alone_at_phone_width():
    phone = " ".join(
        blk
        for blk in _media_blocks(STYLES, "@media (max-width: 480px)")
        if "#back-to-suite" in blk
    )
    assert "#nova-float-btn:not(.nova-btn-close)" in phone
    assert "display: none !important" in phone


def test_step5_buttons_share_one_height_and_the_hint_sits_under_generate():
    heights = [
        b
        for sel in (".studio #step5 .btn-row > .btn", ".studio #step5 .btn-row #generateBtn")
        for b in _rule_bodies(STYLES, sel)
        if "height: 48px" in b
    ]
    assert len(heights) >= 2
    cell = re.search(r'<div class="generate-cell">(.*?)\n      </div>', BODY, re.S)
    assert cell, "no .generate-cell"
    assert 'id="generateBtn"' in cell.group(1) and 'id="cmdEnterHint"' in cell.group(1)
    # a keyboard shortcut means nothing on a touch screen
    assert any(
        "display: none !important" in b
        for blk in _media_blocks(STYLES, "@media (hover: none), (pointer: coarse)")
        for b in _rule_bodies(blk, "#cmdEnterHint")
    )
    assert "(hover: none), (pointer: coarse)" in A11Y


def test_post_generation_scroll_target_clears_the_sticky_stack():
    wide = [b for b in _rule_bodies(STYLES, "#planDashboardSection") if "scroll-margin-top: 70px" in b]
    narrow = [
        b
        for blk in _media_blocks(STYLES, "@media (max-width: 1080px)")
        for b in _rule_bodies(blk, "#planDashboardSection")
        if "scroll-margin-top: calc(58px + var(--np-bar-h, 46px) + 8px)" in b
    ]
    assert wide and narrow


# ── toasts and the status strip (round 3) ────────────────────────────────

_TOAST_STUB = r"""
function mkToast(msg, field, ageMs) {
  var t = { dataset: { field: field || '', t: String(Date.now() - (ageMs || 0)) }, msg: msg, gone: false,
    classList: { add: function () { t.exiting = true; }, contains: function () { return false; } },
    remove: function () { t.gone = true; } };
  if (!field) delete t.dataset.field;
  return t;
}
var TOASTS = [];
var document = { querySelectorAll: function (sel) {
  return TOASTS.filter(function (t) { return !t.gone && !t.exiting; }); } };
function setTimeout(fn) { fn(); }
"""


@needs_node
def test_validation_toasts_go_when_their_field_is_edited_or_the_step_changes():
    script = "\n".join(
        [
            _TOAST_STUB,
            js_block(APP, "function novaDismissErrorToasts("),
            js_block(APP, "function novaTagNewestErrorToast("),
            "var a = mkToast('client name', 'clientName'), b = mkToast('use case', 'useCase');",
            "var stale = mkToast('network error', '', 60000), fresh = mkToast('role needed', '', 100);",
            "TOASTS = [a, b, stale, fresh];",
            # a fresh untagged toast is adopted by the field that was just flagged...
            "novaTagNewestErrorToast('roleInput');",
            # ...an old, unrelated one is not (blur-time validation must not adopt it)
            "var tagged = { fresh: fresh.dataset.field, stale: stale.dataset.field || '' };",
            "novaDismissErrorToasts('clientName');",
            "var afterField = TOASTS.filter(function (t) { return !t.gone && !t.exiting; }).map(function (t) { return t.msg; });",
            "novaDismissErrorToasts();",
            "var afterStep = TOASTS.filter(function (t) { return !t.gone && !t.exiting; }).length;",
            "process.stdout.write(JSON.stringify({ tagged: tagged, afterField: afterField, afterStep: afterStep }));",
        ]
    )
    got = run_node(script)
    assert got["tagged"] == {"fresh": "roleInput", "stale": ""}
    assert got["afterField"] == ["use case", "network error", "role needed"]
    assert got["afterStep"] == 0
    # wiring: field edits and a CHANGE of the active step (not other class churn)
    assert 'document.addEventListener(\n    "input"' in APP
    watch = js_block(APP, "(function watchStepChangesForToasts()")
    assert "now !== lastActive" in watch and "novaDismissErrorToasts()" in watch
    assert "return toast;" in js_block(APP, "function showToast(")


def test_form_errors_tag_their_toast_with_the_field():
    assert "errorToast.dataset.field = fieldId" in js_block(APP, "function showFormError(")
    assert "novaTagNewestErrorToast(el.id)" in js_block(APP, "function _showFieldError(")
    assert "novaTagNewestErrorToast(field.id)" in js_block(APP, "function validateField(")


_STRIP_STUB = r"""
var estimating = false;
var classes = {}, attrs = {};
var pane = { classList: { toggle: function (c, on) { classes[c] = on; } } };
var strip = { setAttribute: function (k, v) { attrs[k] = v; } };
function $(id) { return id === 'planPreview' ? pane : id === 'npAgentStrip' ? strip : null; }
"""


@needs_node
def test_the_updating_strip_is_hidden_from_assistive_tech_while_invisible():
    script = "\n".join(
        [
            _STRIP_STUB,
            js_block(PREVIEW, "function setEstimating("),
            "setEstimating(true); var on = { hidden: attrs['aria-hidden'], rc: classes.recomputing };",
            "setEstimating(false); var off = { hidden: attrs['aria-hidden'], rc: classes.recomputing };",
            "process.stdout.write(JSON.stringify({ on: on, off: off }));",
        ]
    )
    assert run_node(script) == {
        "on": {"hidden": "false", "rc": True},
        "off": {"hidden": "true", "rc": False},
    }
    assert re.search(r'id="npAgentStrip"[^>]*aria-hidden="true"', BODY, re.S)


# ══════════════════════════════════════════════════════════════════════════
# Round 4: stale estimates, goal wording, Ask Nova, stepper, tap targets
# ══════════════════════════════════════════════════════════════════════════


_RETRY_STUB = r"""
var timers = [], calls = 0, trace = [];
var estimating = false, latestEstimate = null, latestEstimateSig = '';
var lastScheduledSig = '', estimateRetrySig = '', estimateRetryCount = 0, estimateTimer = null;
var ESTIMATE_DEBOUNCE_MS = 500, ESTIMATE_RETRY_DELAY_MS = 600, ESTIMATE_MAX_AUTO_RETRIES = 3;
var MODE = '__MODE__';
function setTimeout(fn, ms) { timers.push({ fn: fn, ms: ms }); return timers.length; }
function clearTimeout() { timers = []; }
function estimatePayload(m) { return m.inputs; }
function dropStaleFunded() { return 'set'; }
function setFunded() {}
function setEstimating(on) { estimating = !!on; }
function paintOutcomes() { trace.push({ estimating: estimating, est: !!latestEstimate }); }
function gather() { return M; }
function getCsrfToken() { return 'tok'; }
var M = { total: 60000, inputs: { budget: 60000, target_roles: ['Nurse'], locations: [] } };
function fetch() {
  calls++;
  if (MODE === 'down') return Promise.reject(new Error('down'));
  return Promise.resolve({ ok: true, json: function () { return Promise.resolve({ est_hires: 6 }); } });
}
async function settle() { for (var i = 0; i < 8; i++) await Promise.resolve(); }
async function run() {
  fetchEstimate(M);
  // drive the fake clock until nothing is scheduled
  for (var guard = 0; guard < 40 && timers.length; guard++) {
    var t = timers.shift(); t.fn(); await settle();
  }
  process.stdout.write(JSON.stringify({ calls: calls, trace: trace, estimating: estimating, est: latestEstimate, sig: latestEstimateSig, retries: estimateRetryCount }));
}
"""


def _retry_run(mode: str) -> dict:
    return run_node(
        "\n".join(
            [
                _RETRY_STUB.replace("__MODE__", mode),
                js_block(PREVIEW, "function fetchEstimate("),
                "run();",
            ]
        )
    )


@needs_node
def test_a_down_estimate_server_stays_updating_through_every_retry_then_gives_up():
    """Round-4: each failure flipped the bar to dashes for a tick before the
    retry put "Updating estimate…" back. The state is "updating" while a retry
    is still coming, and only the LAST failure is "unavailable"."""
    got = _retry_run("down")
    assert got["calls"] == 1 + 3  # the request + ESTIMATE_MAX_AUTO_RETRIES retries, then it stops
    assert got["estimating"] is False and got["est"] is None and got["sig"] == ""
    trace = got["trace"]
    assert trace[-1] == {"estimating": False, "est": False}
    # every paint before the final verdict said "updating", never "failed"
    assert all(step["estimating"] for step in trace[:-1]), trace
    assert sum(1 for step in trace if not step["estimating"]) == 1


@needs_node
def test_a_successful_estimate_records_the_inputs_it_answers():
    got = _retry_run("ok")
    assert got["calls"] == 1
    assert got["est"] == {"est_hires": 6}
    assert json.loads(got["sig"]) == {"budget": 60000, "target_roles": ["Nurse"], "locations": []}
    assert got["trace"][-1] == {"estimating": False, "est": True}


def test_the_schedule_and_failure_paths_clear_the_estimates_signature():
    fetch = js_block(PREVIEW, "function fetchEstimate(")
    assert fetch.count('latestEstimateSig = "";') >= 3  # no inputs, pending, failed
    assert "latestEstimateSig = latestEstimate ? requestSig" in fetch
    # every reader goes through estimateFor(), none reads the raw estimate
    for fn in ("function paintOutcomes(", "function paintEstimateBar(", "function feasibility("):
        assert "latestEstimate" not in js_block(PREVIEW, fn).replace("latestEstimate.est", ""), fn


_INSIGHT_STUB = r"""
function trim(x, d) { return Number(x.toFixed(d)).toString(); }
function escapeHtml(s) { return String(s); }
function hero(v) { return '$' + Math.round(v / 1000) + 'K'; }
function intc(v) { return Math.round(v).toLocaleString('en-US'); }
function channelOn() { return false; }
function sym() { return '$'; }
"""


def _insights(goal_range: str, target: int, est_hires: float, est_cph: float = 3500, benchmark=None, hire_basis=None, projection=None):
    m = {
        "role": "Nurse", "total": 35000, "targetHires": target, "goalRange": goal_range,
        "roleClass": "skilled", "rows": [{}, {}], "recommended": True, "channelsTouched": False,
        "startMonth": 0,
    }
    script = "\n".join(
        [
            _INSIGHT_STUB,
            js_block(PREVIEW, "function goalText("),
            js_block(PREVIEW, "function hireBasisText("),
            js_block(PREVIEW, "function budgetGap("),
            js_block(PREVIEW, "function computeInsights("),
            f"process.stdout.write(JSON.stringify(computeInsights({json.dumps(m)}, "
            f"{{ est_hires: {est_hires}, est_cph: {json.dumps(est_cph)}, cph_benchmark: {json.dumps(benchmark)}"
            f"{', hire_basis: ' + json.dumps(hire_basis) if hire_basis else ''}"
            f"{', projection: ' + json.dumps(projection) if projection else ''} }})"
            ".map(function (i) { return i.text; })));",
        ]
    )
    return run_node(script)


@needs_node
def test_insights_word_a_picked_range_as_the_range():
    for est_hires in (6.0, 70.0, 200.0):  # short of it, on track, comfortably over
        ranged = _insights("50–100", 75, est_hires)[0]
        assert "50–100 hire goal (planned at ~75)" in ranged, ranged
        assert "75-hire" not in ranged and "75 target" not in ranged
        exact = _insights("", 75, est_hires)[0]
        assert "75-hire target" in exact and "planned at" not in exact, exact


@needs_node
def test_the_q1_seasonality_insight_quotes_no_unsourced_percentage():
    m = {
        "role": "Nurse", "total": 35000, "targetHires": 0, "goalRange": "", "roleClass": "skilled",
        "rows": [{}, {}], "recommended": True, "channelsTouched": False, "startMonth": 2,
    }
    script = "\n".join(
        [
            _INSIGHT_STUB,
            js_block(PREVIEW, "function goalText("),
            js_block(PREVIEW, "function hireBasisText("),
            js_block(PREVIEW, "function budgetGap("),
            js_block(PREVIEW, "function computeInsights("),
            f"process.stdout.write(JSON.stringify(computeInsights({json.dumps(m)}, null)"
            ".map(function (i) { return i.text; })));",
        ]
    )
    text = " ".join(run_node(script))
    assert "peak hiring season" in text and "%" not in text


def test_loading_screen_does_not_claim_a_linkedin_inspired_deck():
    facts = js_block(APP, "const GEN_FACTS")
    assert "LinkedIn" not in facts
    assert "Joveo's 2026 deck design" in facts


# ── Ask Nova (top bar + results screen) ────────────────────────────────────

_ASK_STUB = r"""
var P = __PARAMS__;
var _lastPlanPayload = P.payload;
var opened = [], stored = {}, removed = [];
var window = { open: function (u, t) { opened.push([u, t]); },
  NovaPreview: { planIsCurrent: function () { return P.current; } } };
var sessionStorage = { setItem: function (k, v) { stored[k] = JSON.parse(v); },
  removeItem: function (k) { removed.push(k); delete stored[k]; } };
var selectedIndustry = P.industryKey || null;
var channelsDB = P.industryKey ? { industries: (function () { var o = {}; o[P.industryKey] = { label: P.industry }; return o; })() } : null;
var selectedNaicsTitle = P.naics || '';
function novaPlanCoreInputs() { return P.inputs; }
function novaResolvePlanBudget(raw) { return { ok: /^[0-9,]+$/.test(raw) }; }
function novaPlanBudgetLabel(p) { return '$' + Math.round(Number(String(p.budget_range).replace(/,/g, '')) / 1000) + 'K'; }
function getSelectedRegion() { return { region: 'us_only', custom_countries: [] }; }
var document = { querySelector: function () {
  return P.card ? { querySelector: function () { return { textContent: P.card }; } } : null; } };
"""


def _ask(payload, inputs=None, industry="", current=True, naics="", card="", key="healthcare_medical"):
    params = {
        "payload": payload, "inputs": inputs or {}, "industry": industry,
        "industryKey": key if industry else "", "current": current, "naics": naics, "card": card,
    }
    script = "\n".join(
        [
            _ASK_STUB.replace("__PARAMS__", json.dumps(params)),
            js_block(APP, "function novaWizardIndustryLabel("),
            js_block(APP, "function novaForgetPlanContext("),
            js_block(APP, "function novaWizardBriefMessage("),
            js_block(APP, "function askNovaAboutPlan("),
            "askNovaAboutPlan();",
            "process.stdout.write(JSON.stringify({ opened: opened, stored: stored }));",
        ]
    )
    return run_node(script)


def _prefill(url: str) -> str:
    from urllib.parse import parse_qs, urlparse

    return parse_qs(urlparse(url).query).get("prefill", [""])[0]


_WIZARD_INPUTS = {
    "client_name": "Acme Health",
    "budget_range": "60,000",
    "budget_period": "campaign",
    "campaign_duration": "3-6 months",
    "target_roles": ["Registered Nurse"],
    "locations": ["Los Angeles, CA"],
}


@needs_node
def test_ask_nova_before_a_plan_exists_prefills_the_wizard_inputs_and_does_not_send():
    got = _ask(None, _WIZARD_INPUTS, "Healthcare & Medical")
    (url, target), = got["opened"]
    assert target == "_blank" and url.startswith("/nova?prefill=") and url.endswith("&send=0")
    message = _prefill(url)
    assert message.startswith("I'm planning a Healthcare & Medical campaign for Acme Health: ")
    assert "budget $60K" in message and "roles Registered Nurse" in message
    assert "locations Los Angeles, CA" in message
    assert "I just generated" not in message and "details: ." not in message
    assert got["stored"]["nova_plan_context"]["autosend"] is False


@needs_node
def test_ask_nova_names_the_industry_however_it_was_set():
    """Round-5: the drafted question read "a campaign" when the industry came
    from an example chip / restored draft (no highlighted card to read)."""
    # the industry key the wizard holds, via the channel DB's label
    msg = _prefill(_ask(None, _WIZARD_INPUTS, "Healthcare & Medical")["opened"][0][0])
    assert "a Healthcare & Medical campaign for Acme Health" in msg
    # an "a"/"an" that agrees with the word
    msg = _prefill(_ask(None, _WIZARD_INPUTS, "Education", key="education")["opened"][0][0])
    assert "an Education campaign for Acme Health" in msg
    # no key, but a precise NAICS pick
    msg = _prefill(_ask(None, _WIZARD_INPUTS, "", naics="Offices of Physicians")["opened"][0][0])
    assert "an Offices of Physicians campaign" in msg or "a Offices of Physicians campaign" in msg
    # no key, no NAICS: the highlighted card's own text
    msg = _prefill(_ask(None, _WIZARD_INPUTS, "", card="Retail")["opened"][0][0])
    assert "a Retail campaign for Acme Health" in msg


@needs_node
def test_ask_nova_with_nothing_filled_in_just_opens_the_chat():
    got = _ask(None, {"client_name": "", "target_roles": [], "locations": [], "budget_range": ""})
    assert got["opened"] == [["/nova", "_blank"]]  # no prefill: never an empty-details message
    assert got["stored"] == {}


@needs_node
def test_ask_nova_after_a_generated_plan_describes_that_plan_and_sends():
    payload = dict(_WIZARD_INPUTS, industry_label="Healthcare", hire_volume="50-100 hires")
    got = _ask(payload, _WIZARD_INPUTS, "Healthcare", current=True)
    (url, _), = got["opened"]
    assert "send=0" not in url
    message = _prefill(url)
    assert message.startswith("I just generated a media plan with these details: Client: Acme Health")
    assert "Roles: Registered Nurse" in message and "Hire volume: 50-100 hires" in message
    assert "details: ." not in message
    assert got["stored"]["nova_plan_context"]["autosend"] is True


@needs_node
def test_ask_nova_never_auto_sends_a_plan_that_is_no_longer_the_one_on_screen():
    """Round-5: after Clear Form / a new client, the old plan's payload is still
    cached; it must not be sent for the user."""
    payload = dict(_WIZARD_INPUTS, client_name="Old Client", industry_label="Healthcare")
    got = _ask(payload, dict(_WIZARD_INPUTS, client_name="New Client"), "Healthcare", current=False)
    (url, _), = got["opened"]
    message = _prefill(url)
    assert url.endswith("&send=0")
    assert "Old Client" not in message and "I just generated" not in message
    assert message.startswith("I'm planning a Healthcare campaign for New Client")
    assert got["stored"]["nova_plan_context"]["autosend"] is False


@needs_node
def test_ask_nova_with_a_hollow_plan_payload_is_treated_as_no_plan():
    """A payload with none of the fields used to produce "...details: ."."""
    got = _ask({}, _WIZARD_INPUTS, "Healthcare")
    (url, _), = got["opened"]
    assert url.endswith("&send=0") and "I just generated" not in _prefill(url)


def test_the_message_kept_for_the_nova_tab_is_dropped_when_the_form_is_cleared_or_the_client_changes():
    assert "novaForgetPlanContext();" in js_block(APP, "function clearFormState(")
    init = js_block(APP, "function _doInit(")
    assert 'getElementById("clientName")' in init
    assert 'addEventListener("input", novaForgetPlanContext)' in init
    assert 'sessionStorage.removeItem("nova_plan_context")' in js_block(APP, "function novaForgetPlanContext(")


def test_the_nova_page_consumes_the_stored_message_whichever_way_it_was_read():
    nova = (PARTIALS.parent.parent / "nova.html").read_text(encoding="utf-8")
    # a reload of the opened tab (URL already cleaned) must not find it again
    assert 'if (prefillMsg) sessionStorage.removeItem("nova_plan_context");' in nova
    assert 'sessionStorage.removeItem("nova_plan_context");' in nova[nova.index("var stored = storedCtx;") :]


def test_nova_page_only_auto_sends_when_told_to():
    nova = (PARTIALS.parent.parent / "nova.html").read_text(encoding="utf-8")
    assert 'params.get("send") !== "0"' in nova
    assert "ctx.autosend !== false" in nova
    send_block = nova[nova.index("if (autoSend)") :]
    assert "sendBtn.click()" in send_block[: send_block.index("} else {")]


def test_the_css_comment_does_not_claim_ask_nova_opens_the_chat_panel():
    raw = (PARTIALS / "head_styles.html").read_text(encoding="utf-8")
    assert '"Ask Nova" opens the\n     chat)' not in raw
    assert "does not open\n     this in-page chat panel" in raw


# ── stepper + tap targets ─────────────────────────────────────────────────


def test_a_completed_step_label_is_never_clipped_at_phone_width():
    phone = "\n".join(_media_blocks(STYLES, "@media (max-width: 480px)"))
    label = [b for b in _rule_bodies(phone, ".studio .step-label") if "max-width" in b]
    assert label, "the 60px cap on .step-label must be lifted inside .studio"
    assert "max-width: none" in label[0] and "flex-wrap: wrap" in label[0]
    rail = [b for b in _rule_bodies(phone, ".studio .progress-bar") if "gap" in b]
    assert rail and "gap: 4px" in rail[0]


def test_output_format_toggles_are_44px_and_report_their_state():
    base = [b for b in _rule_bodies(STYLES, ".output-format-btn") if "min-height" in b]
    assert base and "min-height: 44px" in base[0]
    assert 'data-format="local"\n          aria-pressed="true"' in BODY
    assert 'data-format="google"\n          aria-pressed="false"' in BODY
    assert '"aria-pressed"' in js_block(FOOTER, "function setOutputFormat(")


def test_clear_form_and_channel_switches_get_44px_hit_areas_on_touch_layouts():
    blocks = list(_media_blocks(STYLES, "@media (max-width: 1080px)"))
    clear = [b for blk in blocks for b in _rule_bodies(blk, ".studio .plan-clear-form")]
    assert any("min-height: 44px" in b for b in clear), clear
    reach = [
        b
        for blk in blocks
        for b in list(_rule_bodies(blk, ".studio .toggle-switch::after"))
        + list(_rule_bodies(blk, ".studio .toggle::after"))
    ]
    assert reach and "inset: -9px -4px" in reach[0]  # 26px switch + 2 x 9px = 44px


# ══════════════════════════════════════════════════════════════════════════
# Round 5: click-to-apply budget, graded cost per hire, typing state
# ══════════════════════════════════════════════════════════════════════════

COPILOT = (PARTIALS / "body_copilot_js.html").read_text(encoding="utf-8")


def _extract(field: str, message: str):
    script = "\n".join(
        [
            js_block(COPILOT, "function nudgeExtractValue("),
            "process.stdout.write(JSON.stringify(nudgeExtractValue("
            f"{json.dumps(field)}, {json.dumps(message)})));",
        ]
    )
    return run_node(script)


@needs_node
def test_no_budget_nudge_is_click_to_apply():
    """Round-5: "Click to apply" on the budget nudge toasted "Recommended
    budget: $56,452" (the user's $35K / 1.24 ... or any first "$" figure in
    the text) beside a Reality Check saying to raise the budget by ~$490K."""
    for message in (
        "In Los Angeles, cost per application runs about 1.24x the national median "
        "(blended C2ER and BLS public data), so your budget stretches like ~$28,226 in a median market.",
        "This budget may be tight for healthcare roles. Consider raising it, or narrowing scope.",
        "Recommended budget: $56,452",
        "$50,000 - $250,000",
    ):
        assert _extract("budget", message) is None, message
    # the apply side has no budget branch either
    assert 'field === "budget"' not in js_block(COPILOT, "function nudgeApplyValue(")
    assert 'field === "budget"' not in js_block(COPILOT, "function nudgeExtractValue(")


@needs_node
def test_a_duration_recommendation_is_still_click_to_apply():
    assert _extract(
        "duration", "Recommended duration for engineering roles: 3-6 months. Technical roles take longer to fill."
    ) == "3-6 months"
    assert _extract("duration", "Short campaigns for sales roles risk low yield: passive candidates take time.") is None


_BENCH_NURSE = {"label": "nursing", "p25": 4200, "median": 5400, "p75": 7800, "source": "Test Source 2025, SHRM 2025"}
_BENCH_TECH = {"label": "tech", "p25": 4200, "median": 5800, "p75": 9500, "source": "Test Source 2025, DHI Group 2024"}


@needs_node
def test_cost_per_hire_is_graded_only_against_the_data_quartiles():
    """Round-5: "High ~$7K/hire for Registered Nurse" came from a hand-typed
    per-role-class table; a (hypothetical, sourced) nursing upper quartile of $7.8K is what the grade reads."""
    def cph_lines(est_cph, benchmark, est_hires=6):
        return [
            t
            for t in _insights("", 0, est_hires, est_cph=est_cph, benchmark=benchmark)
            if "/hire" in t
        ]

    # $7K nurse hire: inside nursing's quartiles (4.2K-7.8K) -> nothing said
    assert cph_lines(7000, _BENCH_NURSE) == []
    # above the p75 -> High, naming the yardstick and its source
    high = cph_lines(9000, _BENCH_NURSE)
    assert len(high) == 1 and high[0].startswith("High <b>~$9K/hire</b>")
    # the sources are the row's OWN list, in full -- never "SHRM" alone
    assert (
        "above the $7.8K upper quartile for nursing "
        "(sources listed in the benchmark file: Test Source 2025, SHRM 2025)"
    ) in high[0]
    # one decimal under $10K: a $7.9K projection is above $7.8K, and says so
    assert cph_lines(7900, _BENCH_NURSE)[0].startswith("High <b>~$7.9K/hire</b>")
    # a tech hire at $7K is fine (p75 $9.5K); at $10K it is high
    assert cph_lines(7000, _BENCH_TECH) == []
    assert cph_lines(10000, _BENCH_TECH)[0].startswith("High")
    # below the p25 -> Efficient
    low = cph_lines(3000, _BENCH_NURSE)
    assert len(low) == 1 and low[0].startswith("Efficient <b>~$3K/hire</b>") and "$4.2K lower quartile" in low[0]
    # no benchmark for these roles / a non-USD plan: no grade at all
    assert cph_lines(20000, None) == []
    # Round-6: below one hire there is no cost per hire to grade (the engine's
    # figure there is the whole budget): "Efficient ~$3K/hire ... below the $4.2K
    # lower quartile" / "High ~$9K/hire" stood beside "0 Est. hires" in production
    for budget_as_cph in (3000, 9000):
        assert cph_lines(budget_as_cph, _BENCH_NURSE, est_hires=0) == []
    assert cph_lines(None, _BENCH_NURSE, est_hires=0) == []


def test_the_typed_cost_per_hire_table_is_gone():
    assert "BENCH = {" not in PREVIEW and "14000" not in PREVIEW and "bench * 1.4" not in PREVIEW
    assert "est.cph_benchmark" in js_block(PREVIEW, "function computeInsights(")


_EDIT_STUB = r"""
var lastScheduledSig = JSON.stringify({ s: 'sig-of-35000' }), painted = [];
var M = { sig: 'sig-of-35000' };
function gather() { return M; }
function estimatePayload(m) { return { s: m.sig }; }
function paintOutcomes(m) { painted.push(m.sig); }
"""


@needs_node
def test_the_first_keystroke_that_changes_the_inputs_repaints_at_once():
    """Round-5: the bar stayed "ready" with the old figures while the field
    already read 60000, because render() waits 110ms after the LAST keystroke."""
    def run(sig):
        script = "\n".join(
            [
                _EDIT_STUB,
                js_block(PREVIEW, "function markEdited("),
                f"M.sig = {json.dumps(sig)}; markEdited();",
                "process.stdout.write(JSON.stringify(painted));",
            ]
        )
        return run_node(script)

    assert run("sig-of-60000") == ["sig-of-60000"]  # inputs changed: repaint now
    assert run("sig-of-35000") == []  # a keystroke elsewhere: nothing to say
    assert 'markEdited(); schedule();' in PREVIEW  # wired on every input event


@needs_node
def test_the_bar_is_loading_for_inputs_no_request_exists_for_yet():
    got = _bar_script(
        "ready(" + json.dumps(_EST) + ", 35000, ['Nurse']); scheduled(35000, ['Nurse']);"
        "paint(35000, ['Nurse']); var before = attrs['data-state'];"
        # the user typed 60000: nothing is in flight and no request is scheduled yet
        "estimating = false; paint(60000, ['Nurse']);"
        "var out = { before: before, after: attrs['data-state'], figs: figs() };"
    )
    assert got["before"] == "ready"
    assert got["after"] == "loading"
    assert got["figs"] == {"nebBudget": "$60K", "nebApps": "…", "nebHires": "…", "nebCpa": "…"}


def test_countries_copy_comes_from_the_same_source_as_the_rest_of_the_site():
    """Round-5: "Supports 40+ countries" had no source (country_job_boards
    alone has 34); the count is proof_claims' distinct country keys."""
    assert "40+ countries" not in BODY and "Supports 40" not in BODY
    assert "{{claim_countries}} countries" in BODY
    import proof_claims

    rendered = proof_claims.render("{{claim_countries}} countries")
    assert re.fullmatch(r"\d+ countries", rendered), rendered


# ══════════════════════════════════════════════════════════════════════════
# Round 6: fewer than one hire, pricing from data, step-2 insights, labels
# ══════════════════════════════════════════════════════════════════════════


@needs_node
def test_fewer_than_one_hire_says_so_instead_of_grading_or_going_quiet():
    """Round-6 regression: Nurse / LA / $3K returned est_hires 0 and est_cph =
    the whole budget; the preview graded "Efficient ~$3K/hire" beside "0 Est.
    hires" and the Reality Check vanished exactly when the goal was unreachable."""
    est = {"est_hires": 0, "est_cph": None}
    got = _engine_feasibility(est, 3000, 75, goal_range="50\u2013100")
    assert got == {
        "ratio": 0,
        "belowOne": True,
        "budget": "$3K",
        "goal": "50\u2013100 hire goal (planned at ~75)",
    }
    # ...and the Nova insight uses the same words
    text = _insights("50\u2013100", 75, 0, est_cph=None, benchmark=_BENCH_NURSE)
    assert text == [
        "Budget supports <b>fewer than one hire</b> against your 50\u2013100 hire goal "
        "(planned at ~75) \u2014 raise the budget substantially or narrow scope."
    ]


@needs_node
def test_the_reality_check_banner_for_fewer_than_one_hire():
    html = _feas(
        {"ratio": 0, "belowOne": True, "budget": "$3K", "goal": "50\u2013100 hire goal (planned at ~75)"}
    )["html"]
    assert html is not None
    assert "UNREALISTIC" in html
    assert "<strong>fewer than one hire</strong>" in html
    assert "against your 50\u2013100 hire goal (planned at ~75)" in html
    # nothing computed from a hires figure that does not exist
    assert "/hire" not in html and "raise the budget by" not in html and "NaN" not in html
    assert "raise the budget substantially or narrow the scope" in html


def test_the_estimate_keeps_the_reality_check_when_nothing_is_funded():
    block = js_block(PREVIEW, "function feasibility(")
    assert "belowOne: true" in block
    assert "return null" in block  # still null with no estimate / no goal / no budget


def test_smart_insights_do_not_print_unsourced_figures():
    """Round-6: step 2's "Avg CPA Range $8.00 - $35.00", "Avg Time to Fill
    45-90 days" and "40% awareness / 60% hiring" came from channels_db.json,
    which carries no source (its healthcare CPA sits beside the benchmark file's
    nursing CPA of $28-$68)."""
    block = re.sub(r"//[^\n]*", "", js_block(APP, "function updateJobCategoryInsight("))  # comments explain the omission
    for gone in ("avg_cpa_range", "avg_time_to_fill", "strategy_emphasis", "Avg CPA Range", "Time to Fill", "Strategy Mix"):
        assert gone not in block, gone
    assert "Top Recommendations" in block  # the qualitative part stays


@needs_node
def test_the_pricing_grid_renders_ranges_and_reference_cards_without_trend_or_best_value():
    data = {
        "channels": [
            {"name": "Indeed", "cpc": {"low": 0.3, "median": 0.92, "high": 4.5}, "cpa": {"low": 6, "median": 18, "high": 45}, "source": "Appcast 2025"}
        ],
        "industry": {
            "label": "nursing",
            "cpc": {"low": 2.2, "median": 4.1, "high": 6.8}, "cpc_source": "Appcast 2025",
            "cpa": {"low": 28, "median": 40, "high": 68}, "cpa_source": "Joveo 2026, Appcast 2025",
            "cph": {"low": 4200, "median": 5400, "high": 7800}, "cph_source": "Test Source 2025, SHRM 2025",
        },
        "metro": {"name": "Los Angeles", "cpa_mult": 1.24, "cpc_mult": 1.28, "source": "C2ER 2024 and BLS 2025"},
    }
    script = "\n".join(
        [
            "var grid = { innerHTML: '' };",
            "var document = { getElementById: function (id) { return id === 'pricing-grid' ? grid : null; } };",
            "function escapeHtml(s) { return String(s); }",
            js_block(APP, "function pricingRange("),
            js_block(APP, "function renderPricingGrid("),
            f"renderPricingGrid({json.dumps(data)});",
            "process.stdout.write(JSON.stringify(grid.innerHTML));",
        ]
    )
    html = run_node(script)
    assert "CPC</span><span class=\"value\">$0.30 - $4.50 (median $0.92)" in html
    assert "CPA</span><span class=\"value\">$6 - $45 (median $18)" in html
    assert "nursing (industry reference, 25th - 75th percentile)" in html
    assert "$4,200 - $7,800 (median $5,400)" in html and "Test Source 2025, SHRM 2025" in html
    assert "Los Angeles (local multiplier vs the national median)" in html
    assert "1.24x" in html and "1.28x" in html
    assert "cost-of-living-based estimate (C2ER 2024 and BLS 2025)" in html
    assert "not applied to the ranges above" in html
    assert "Best Value" not in html and "trend-" not in html


# -- hire estimates state the cost-per-hire basis they divide by -------------

_HIRE_BASIS = {
    "midpoint_cph": 10500.0,
    "floor_cph": 5250.0,
    "label": "assumed all-in industry cost per hire (Nova table, unsourced)",
}
_HIRES_RANGE = {"hires": {"low": 11, "mid": 22, "high": 22}}


def _no_us_only_marker(text: str) -> None:
    from us_only_markers import _US_ONLY_MARKERS

    hits = [m for m in _US_ONLY_MARKERS if m.lower() in text.lower()]
    assert not hits, (hits, text)


@needs_node
def test_insight_never_says_even_at_the_conservative_end_and_names_its_basis():
    # target 10 sits at/below the low end (11) of the estimated range
    with_basis = _insights("", 10, 11.0, hire_basis=_HIRE_BASIS, projection=_HIRES_RANGE)[0]
    assert "even at the conservative end" not in with_basis
    assert "if hires cost the assumed" in with_basis and "$10.5K" in with_basis
    assert "all-in" in with_basis and "Nova table" in with_basis
    _no_us_only_marker(with_basis)
    # no basis from the server -> no "covers" reassurance at all
    bare = _insights("", 10, 11.0)[0]
    assert "even at the conservative end" not in bare and "covers" not in bare.lower()
    _no_us_only_marker(bare)


@needs_node
def test_hire_basis_note_text_for_a_range_and_a_point():
    def note(est: dict) -> str:
        return run_node(
            "\n".join(
                [
                    "function sym() { return '$'; }",
                    "function trim(x, d) { return Number(x.toFixed(d)).toString(); }",
                    js_block(PREVIEW, "function hireBasisText("),
                    f"process.stdout.write(JSON.stringify(hireBasisText({json.dumps(est)})));",
                ]
            )
        )

    ranged = note({"hire_basis": _HIRE_BASIS, "projection": _HIRES_RANGE})
    assert ranged == (
        "low end at an assumed $10.5K all-in cost per hire (Nova table), high end at $5.25K"
    )
    point = note({"hire_basis": _HIRE_BASIS, "projection": {"hires": {"low": 11, "mid": 11, "high": 11}}})
    assert point == "at an assumed $10.5K all-in cost per hire (Nova table)"
    assert note({}) == "" and note({"hire_basis": None}) == ""
    for t in (ranged, point):
        _no_us_only_marker(t)


@needs_node
def test_estimate_bar_states_the_basis_beside_the_hires_range():
    est = dict(_EST, hire_basis=_HIRE_BASIS, projection=_HIRES_RANGE)
    got = _bar_script(
        "ready(" + json.dumps(est) + ", 35000, ['Nurse']); paint(35000, ['Nurse']);"
        "var out = { attrs: attrs, basis: texts.nebBasis.textContent };"
    )
    assert "assumed $10.5K all-in cost per hire (Nova table)" in got["basis"]
    assert "assumed $10.5K all-in cost per hire (Nova table)" in got["attrs"]["aria-label"]
    # no basis from the server -> nothing is invented
    got = _bar_script(
        "ready(" + json.dumps(_EST) + ", 35000, ['Nurse']); paint(35000, ['Nurse']);"
        "var out = { attrs: attrs, basis: texts.nebBasis.textContent };"
    )
    assert got["basis"] == ""
    # and the pane's basis line reads the same helper
    assert "hireBasisText(est)" in js_block(PREVIEW, "function paintOutcomes(")


# -- Budget Reality Check: basis clause + a target-based gap ------------------

_FEAS_BASIS_EST = {
    "est_hires": 6.0,
    "est_cph": 5645,
    "hire_basis": _HIRE_BASIS,
    "projection": {"hires": {"low": 3, "mid": 6, "high": 6}},
}


@needs_node
def test_reality_check_banner_carries_the_hire_basis_clause():
    basis = "low end at an assumed $10.5K all-in cost per hire (Nova table), high end at $5.25K"
    html = _feas(dict(_FEAS_FIGURES, basis=basis))["html"]
    assert "Hires: " + basis + "." in html
    _no_us_only_marker(html)
    # the server sent no basis: nothing is invented
    assert "Hires: " not in _feas(_FEAS_FIGURES)["html"]
    # and the same helper words it in the engine's feasibility()
    got = _engine_feasibility(_FEAS_BASIS_EST, 35000, 75)
    assert got["basis"] == basis.replace("low end at ", "low end at ")  # range -> two-ended note
    assert _engine_feasibility({"est_hires": 6.0, "est_cph": 5645}, 35000, 75)["basis"] == ""


@needs_node
def test_reality_check_gap_is_target_times_assumed_cost_minus_budget():
    """The old gap, total * (target / hires - 1), GROWS as the budget typed
    grows (hires scale with it); the dollars still needed for a fixed target
    can only shrink."""
    a = _engine_feasibility(_FEAS_BASIS_EST, 35000, 75)
    assert a["gap"] == "$753K"  # 75 x 10,500 - 35,000 = 752,500
    big = dict(_FEAS_BASIS_EST, est_hires=12.0, projection={"hires": {"low": 6, "mid": 12, "high": 12}})
    b = _engine_feasibility(big, 70000, 75)
    assert b["gap"] == "$718K"  # 75 x 10,500 - 70,000 = 717,500
    # without a basis the engine's own hires drive it (legacy behaviour)
    legacy = _engine_feasibility({"est_hires": 6.0, "est_cph": 5645}, 35000, 75)
    assert legacy["gap"] == "$403K"
    # the Nova insight quotes the same gap
    ins = _insights("", 75, 6.0, hire_basis=_HIRE_BASIS, projection={"hires": {"low": 3, "mid": 6, "high": 6}})[0]
    assert "$" in ins and "Hires: " in ins
    _no_us_only_marker(ins)


def test_sticky_offsets_read_the_bars_rendered_height():
    """The basis line makes the bar taller than 46px; the scroll-margin offsets
    that clear it follow the measured height (--np-bar-h), not a typed 112px."""
    assert "112px" not in STYLES.split("#planDashboardSection", 1)[1][:400]
    assert STYLES.count("calc(58px + var(--np-bar-h, 46px) + 8px)") == 2
    block = js_block(PREVIEW, "function syncEstimateBarHeight(")
    assert "offsetHeight" in block and "--np-bar-h" in block
    assert "syncEstimateBarHeight();" in js_block(PREVIEW, "function paintEstimateBar(")
    assert 'addEventListener("resize", syncEstimateBarHeight)' in PREVIEW


_LOCAL_BASIS = {
    "midpoint_cph": 15500.0,
    "floor_cph": 7750.0,
    "basis": "local_kb",
    "sources": ["Eurojob-Consulting"],
    "label": "cited local cost-per-hire range midpoint (Eurojob-Consulting)",
}


@needs_node
def test_a_cited_local_basis_is_named_and_never_called_the_nova_table():
    def note(est: dict) -> str:
        return run_node(
            "\n".join(
                [
                    "function sym() { return '\u20ac'; }",
                    "function trim(x, d) { return Number(x.toFixed(d)).toString(); }",
                    js_block(PREVIEW, "function hireBasisText("),
                    f"process.stdout.write(JSON.stringify(hireBasisText({json.dumps(est)})));",
                ]
            )
        )

    text = note({"hire_basis": _LOCAL_BASIS, "projection": {"hires": {"low": 5, "mid": 10, "high": 10}}})
    assert "Eurojob-Consulting" in text and "\u20ac15.5K" in text and "midpoint" in text
    assert "Nova table" not in text and "assumed" not in text and "all-in" not in text
    _no_us_only_marker(text)
    # the insight names the source too, with no Nova-table wording
    ins = _insights("", 5, 5.0, hire_basis=_LOCAL_BASIS, projection={"hires": {"low": 5, "mid": 10, "high": 10}})[0]
    assert "Eurojob-Consulting" in ins and "Nova table" not in ins and "assumed" not in ins
    # the pane line drops "the table has no external source" for a cited basis
    pane = js_block(PREVIEW, "function paintOutcomes(")
    assert 'basis !== "nova_table" ? "." : "; the table has no external source."' in pane


@needs_node
def test_a_kb_row_basis_is_named_in_the_wizard_without_the_nova_table_claim():
    label = ("Nova working estimate from the knowledge base's '$3,000-$5,000 (food manufacturing)' row "
             "(floor: half the cross-industry default)")
    hb = {"midpoint_cph": 4000.0, "floor_cph": 3000.0, "basis": "kb_row", "sources": [], "label": label}
    proj = {"hires": {"low": 30, "mid": 40, "high": 40}}
    text = run_node(
        "\n".join(
            [
                "function sym() { return '$'; }",
                "function trim(x, d) { return Number(x.toFixed(d)).toString(); }",
                js_block(PREVIEW, "function hireBasisText("),
                f"process.stdout.write(JSON.stringify(hireBasisText({json.dumps({'hire_basis': hb, 'projection': proj})})));",
            ]
        )
    )
    assert "food manufacturing" in text and "$4K" in text
    assert "Nova table" not in text and "all-in" not in text
    ins = _insights("", 10, 30.0, hire_basis=hb, projection=proj)[0]
    assert "food manufacturing" in ins and "Nova table" not in ins and "all-in" not in ins
