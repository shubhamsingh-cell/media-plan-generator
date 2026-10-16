"""The two /api/export/status pollers wait for the CSRF token.

Pre-fix, ``body_app_js.html`` and ``body_footer_scripts.html`` POSTed
/api/export/status at parse time with ``getCsrfToken()`` read once, while the
fetch to /api/csrf-token in ``body_csrf_js.html`` was still in flight. The
header was empty (first visit) or an expired cookie, so the server's
double-submit check answered 403 on ~1/3 of loads (prod 2026-10-16: 22 of 66
calls) and the Google output toggle read "not configured".

The real template JS runs under node against a stubbed fetch/cookie jar.
"""

from __future__ import annotations

import json

import pytest

from tests.wizard_js_harness import NODE, PARTIALS, run_node

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")

CSRF_JS = (PARTIALS / "body_csrf_js.html").read_text(encoding="utf-8")
APP_JS = (PARTIALS / "body_app_js.html").read_text(encoding="utf-8")
FOOTER_JS = (PARTIALS / "body_footer_scripts.html").read_text(encoding="utf-8")


def _iife(src: str, start: str) -> str:
    i = src.index(start)
    return src[i : src.index("})();", i) + len("})();")]


def _csrf_script() -> str:
    body = CSRF_JS.split("<script>", 1)[1]
    return body.rsplit("</script>", 1)[0]


POLLERS = {
    "app": _iife(APP_JS, "(function checkExportStatus() {"),
    "footer": _iife(FOOTER_JS, "(function checkGoogleExportAvailability() {"),
}

HARNESS = r"""
var CSRF = @@CSRF@@;
var POLLERS = @@POLLERS@@;
var sleep = function (ms) { return new Promise(function (r) { setTimeout(r, ms); }); };
function resp(status, body) {
  return { status: status, ok: status < 400,
    json: function () { return Promise.resolve(body); },
    clone: function () { return resp(status, body); } };
}
async function scenario(o) {
  var cookie = o.initialCookie, tokN = 0, csrfCalls = 0, statusCalls = 0;
  var release = null, statuses = [], sheetsShown = false, googleDisabled = false;
  var document = {
    get cookie() { return cookie ? "a=b; csrf_token=" + cookie : "a=b"; },
    getElementById: function (id) {
      return {
        classList: { add: function () { googleDisabled = true; } },
        style: { set display(v) { sheetsShown = true; } },
      };
    },
  };
  function rotate() { tokN += 1; cookie = "tok" + tokN; return resp(200, { token: cookie }); }
  function fetchStub(url, opts) {
    if (url === "/api/csrf-token") {
      csrfCalls += 1;
      if (o.csrfRejects) return Promise.reject(new Error("offline"));
      if (o.deferFirstCsrf && csrfCalls === 1) {
        return new Promise(function (res) { release = function () { res(rotate()); }; });
      }
      return Promise.resolve(rotate());
    }
    statusCalls += 1;
    var header = (opts.headers || {})["X-CSRF-Token"] || "";
    var valid = header !== "" && header === cookie && header.indexOf("tok") === 0 &&
      !o.always403 && !(o.fail403First && statusCalls === 1);
    statuses.push({ header: header, cookieAtSend: cookie || "" });
    return Promise.resolve(valid ? resp(200, { configured: true }) :
      resp(403, { success: false, code: "AUTH_REQUIRED",
        error: "Invalid or missing CSRF token. Refresh the page and try again." }));
  }
  var code = "var _googleExportConfigured = false;\n" + CSRF +
    o.pollers.map(function (k) { return POLLERS[k]; }).join("\n") +
    "\nreturn window.__csrfReady;";
  var window = {};
  var ready = new Function("window", "document", "fetch", "showToast", code)(
    window, document, fetchStub, function () {});
  await sleep(30);
  var beforeRelease = statusCalls;
  var readyState = "pending";
  if (ready && ready.then) { ready.then(function () { readyState = "resolved"; },
    function () { readyState = "rejected"; }); }
  if (release) { release(); release = "done"; }
  await sleep(60);
  return { beforeRelease: beforeRelease, statuses: statuses, csrfCalls: csrfCalls,
    statusCalls: statusCalls, sheetsShown: sheetsShown, googleDisabled: googleDisabled,
    readyState: ready && ready.then ? readyState : "missing" };
}
(async function () {
  var out = {};
  var S = @@SCENARIOS@@;
  for (var k in S) out[k] = await scenario(S[k]);
  console.log(JSON.stringify(out));
})();
"""

SCENARIOS = {
    # first visit / expired cookie, token fetch still in flight at parse time
    "race": {
        "pollers": ["app", "footer"],
        "initialCookie": "expired",
        "deferFirstCsrf": True,
    },
    # token rotated under the page: first POST 403s with the CSRF body once
    "retry": {"pollers": ["app"], "initialCookie": "", "fail403First": True},
    # the server keeps saying 403: one retry, never a loop
    "bounded": {"pollers": ["footer"], "initialCookie": "", "always403": True},
    # /api/csrf-token unreachable: readiness must still resolve
    "offline": {"pollers": ["app"], "initialCookie": "", "csrfRejects": True},
}


@pytest.fixture(scope="module")
def results() -> dict:
    script = (
        HARNESS.replace("@@CSRF@@", json.dumps(_csrf_script()))
        .replace("@@POLLERS@@", json.dumps(POLLERS))
        .replace("@@SCENARIOS@@", json.dumps(SCENARIOS))
    )
    return run_node(script)


@needs_node
def test_no_export_status_request_before_csrf_token_resolves(results: dict) -> None:
    r = results["race"]
    assert r["beforeRelease"] == 0, (
        f"{r['beforeRelease']} /api/export/status POST(s) left before "
        "/api/csrf-token answered"
    )


@needs_node
def test_header_equals_cookie_at_send_time(results: dict) -> None:
    r = results["race"]
    assert len(r["statuses"]) == 2, r  # one per poller
    for s in r["statuses"]:
        assert s["header"] and s["header"] == s["cookieAtSend"], s
    assert r["sheetsShown"] is True
    assert r["googleDisabled"] is False, "Google toggle wrongly read 'not configured'"


@needs_node
def test_one_retry_after_csrf_403(results: dict) -> None:
    r = results["retry"]
    assert r["statusCalls"] == 2, r
    assert r["csrfCalls"] == 2, "the retry must refresh the token first"
    last = r["statuses"][-1]
    assert last["header"] == last["cookieAtSend"] == "tok2", last
    assert r["sheetsShown"] is True


@needs_node
def test_retry_is_bounded_to_one(results: dict) -> None:
    assert results["bounded"]["statusCalls"] == 2, results["bounded"]


@needs_node
def test_csrf_ready_resolves_when_token_fetch_fails(results: dict) -> None:
    r = results["offline"]
    assert r["readyState"] == "resolved", r
    assert (
        r["statusCalls"] >= 1
    ), "poller must still run (and show 'not configured' honestly)"
