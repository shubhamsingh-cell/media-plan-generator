"""Regression tests: saved plans (POST/GET /api/saved-plans) must work AND fail closed.

Prod telemetry 2026-10-01 (ledger A_prod_telemetry section 2 row 22): since
2026-04-07 three call sites in ``MediaPlanHandler`` invoked
``_check_joveo_auth(self)`` as a bare function although it is a METHOD
(``def _check_joveo_auth(self)``). Every request raised ``NameError``:

* ``POST /api/saved-plans``  -> 500 ("Save plan error: name '_check_joveo_auth'
  is not defined"),
* ``GET /api/saved-plans``   -> the outer ``except`` swallowed it and returned
  ``200 {"plans": []}`` -- users believed they had no saved plans,
* ``GET /api/saved-plans/<id>`` -> 500.

A second, latent defect sat behind the first: the three ``_send_error`` calls
were ``self._send_error("Authentication required", 401)`` -- i.e. ``401`` landed
in the ``code`` parameter and ``status`` defaulted to 400, so once the NameError
is fixed an unauthenticated caller would be refused with HTTP 400 and a numeric
error code instead of 401 / ``AUTH_REQUIRED``.

These tests drive the REAL request handler (``app.ThreadedHTTPServer`` +
``app.MediaPlanHandler`` on an ephemeral port, same pattern as
tests/test_api_chat_origin.py) with an in-memory stand-in for the Supabase
client, and additionally pin the bug CLASS with an AST check (no bare call to a
name that is defined only as a class method).
"""

from __future__ import annotations

import ast
import builtins
import hashlib
import hmac
import http.client
import itertools
import json
import socket
import threading
import time
from pathlib import Path
from typing import Any, Iterator, Optional

import pytest

import app

PROJECT_ROOT = Path(__file__).resolve().parent.parent

_SESSION_SECRET = "unit-test-session-signing-secret"
_API_KEY = "unit-test-widget-api-key"
_EMAIL = "tester@joveo.com"


# ---------------------------------------------------------------------------
# In-memory Supabase stand-in (chainable PostgREST-style builder)
# ---------------------------------------------------------------------------


class _Result:
    def __init__(self, data: list[dict[str, Any]]) -> None:
        self.data = data


class _Query:
    def __init__(self, store: list[dict[str, Any]], ids: Iterator[int]) -> None:
        self._store = store
        self._ids = ids
        self._op = "select"
        self._row: dict[str, Any] = {}
        self._filters: list[tuple[str, Any]] = []
        self._limit: Optional[int] = None

    def insert(self, row: dict[str, Any]) -> "_Query":
        self._op = "insert"
        self._row = dict(row)
        return self

    def select(self, _cols: str) -> "_Query":
        self._op = "select"
        return self

    def eq(self, col: str, val: Any) -> "_Query":
        self._filters.append((col, val))
        return self

    def order(self, _col: str, desc: bool = False) -> "_Query":
        return self

    def limit(self, n: int) -> "_Query":
        self._limit = n
        return self

    def execute(self) -> _Result:
        if self._op == "insert":
            row = dict(self._row)
            row["id"] = next(self._ids)
            row["created_at"] = "2026-10-01T00:00:00Z"
            self._store.append(row)
            return _Result([row])
        rows = [r for r in self._store if all(r.get(c) == v for c, v in self._filters)]
        if self._limit is not None:
            rows = rows[: self._limit]
        return _Result(rows)


class _FakeSupabase:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self._ids = itertools.count(1)

    def table(self, name: str) -> _Query:
        assert name == "nova_saved_plans", name
        return _Query(self.rows, self._ids)


class _ExplodingSupabase:
    def table(self, _name: str) -> Any:
        raise RuntimeError("supabase is down")


# ---------------------------------------------------------------------------
# Live server fixture + request helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def live_port() -> Iterator[int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="test-saved-plans-server"
    )
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("test server did not start accepting connections in time")
    yield port
    server.shutdown()
    server.server_close()


_ip_counter = itertools.count(1)


def _fresh_ip() -> str:
    """Unique X-Forwarded-For per request so the per-IP rate limiter never trips."""
    n = next(_ip_counter)
    return f"10.77.{(n // 250) % 250}.{n % 250 + 1}"


def _signed_cookie(email: str = _EMAIL) -> str:
    sig = hmac.new(
        _SESSION_SECRET.encode("utf-8"),
        email.lower().strip().encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"nova_user_email={email}|{sig}"


def _request(
    port: int,
    method: str,
    path: str,
    payload: Optional[dict[str, Any]] = None,
    cookie: str = "",
    headers: Optional[dict[str, str]] = None,
    csrf: bool = True,
) -> tuple[int, dict[str, Any]]:
    hdrs: dict[str, str] = {"X-Forwarded-For": _fresh_ip()}
    hdrs.update(headers or {})
    cookies = [cookie] if cookie else []
    if method == "POST":
        hdrs["Content-Type"] = "application/json"
        if csrf:
            token = app._generate_csrf_token()
            cookies.append(f"csrf_token={token}")
            hdrs["X-CSRF-Token"] = token
    if cookies:
        hdrs["Cookie"] = "; ".join(cookies)
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        conn.request(method, path, body=body, headers=hdrs)
        resp = conn.getresponse()
        raw = resp.read()
    finally:
        conn.close()
    try:
        parsed = json.loads(raw) if raw else {}
    except ValueError:
        parsed = {"_raw": raw.decode("utf-8", "replace")}
    return resp.status, parsed


@pytest.fixture()
def auth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SESSION_SIGNING_SECRET", _SESSION_SECRET)
    monkeypatch.setenv("STRICT_AUTH", "1")
    monkeypatch.setenv("NOVA_API_KEYS", _API_KEY)
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)


@pytest.fixture()
def fake_sb(monkeypatch: pytest.MonkeyPatch) -> _FakeSupabase:
    import supabase_client

    sb = _FakeSupabase()
    monkeypatch.setattr(supabase_client, "get_client", lambda: sb)
    return sb


_PLAN = {
    "name": "Hershey Q4",
    "industry": "food_beverage",
    "location": "Hershey, PA",
    "budget": 90000,
    "data": {"channels": ["LinkedIn", "Indeed"]},
}


# ---------------------------------------------------------------------------
# 1. Fail closed: unauthenticated callers are refused with 401 / AUTH_REQUIRED
# ---------------------------------------------------------------------------


class TestSavedPlansFailClosed:
    def test_post_unauthenticated_is_401(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        status, body = _request(live_port, "POST", "/api/saved-plans", _PLAN)
        assert status == 401, (status, body)
        assert body.get("code") == "AUTH_REQUIRED", body
        assert fake_sb.rows == [], "an unauthenticated POST must not persist anything"

    def test_list_unauthenticated_is_401(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        status, body = _request(live_port, "GET", "/api/saved-plans")
        assert status == 401, (status, body)
        assert body.get("code") == "AUTH_REQUIRED", body
        assert "plans" not in body

    def test_get_one_unauthenticated_is_401(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        fake_sb.rows.append({"id": 7, "user_email": "x@joveo.com", "plan_data": {}})
        status, body = _request(live_port, "GET", "/api/saved-plans/7")
        assert status == 401, (status, body)
        assert body.get("code") == "AUTH_REQUIRED", body
        assert "plan_data" not in body

    def test_forged_unsigned_cookie_is_refused_under_strict_auth(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        status, body = _request(
            live_port,
            "GET",
            "/api/saved-plans",
            cookie="nova_user_email=attacker@joveo.com",
        )
        assert status == 401, (status, body)

    def test_wrong_api_key_is_refused(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        status, _ = _request(
            live_port,
            "GET",
            "/api/saved-plans",
            headers={"X-Nova-Api-Key": "not-the-key"},
        )
        assert status == 401

    def test_non_joveo_signed_cookie_is_refused(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        status, _ = _request(
            live_port,
            "POST",
            "/api/saved-plans",
            _PLAN,
            cookie=_signed_cookie("outsider@example.com"),
        )
        assert status == 401
        assert fake_sb.rows == []

    @pytest.mark.parametrize(
        "spoof",
        [
            {"Origin": "https://geoviz.joveo.com"},
            {"Origin": "https://cg-automation.onrender.com"},
            {"Referer": "https://evil.example/?x=geoviz-3d.vercel.app"},
        ],
    )
    def test_spoofed_widget_origin_header_does_not_open_saved_plans(
        self,
        live_port: int,
        auth_env: None,
        fake_sb: _FakeSupabase,
        spoof: dict[str, str],
    ) -> None:
        """``_check_joveo_auth`` Path 4 trusts a substring of client-settable
        Origin/Referer headers (meant for embedded CG/GeoViz widgets). Saved
        plans are per-user data: with the handlers live again (they were dead
        from 2026-04-07), that path must not apply to them."""
        fake_sb.rows.append(
            {"id": 9, "user_email": "victim@joveo.com", "plan_data": {"secret": 1}}
        )
        for method, path, payload in (
            ("GET", "/api/saved-plans", None),
            ("GET", "/api/saved-plans/9", None),
            ("POST", "/api/saved-plans", _PLAN),
        ):
            status, body = _request(live_port, method, path, payload, headers=spoof)
            assert status == 401, (method, path, spoof, status, body)
            assert body.get("code") == "AUTH_REQUIRED"
            assert "plan_data" not in body and "plans" not in body
        assert len(fake_sb.rows) == 1, "nothing was written"


class _Headers(dict):
    """Case-sensitive stand-in for http.client.HTTPMessage (``get`` only)."""


class _FakeReq:
    def __init__(self, **headers: str) -> None:
        self.headers = _Headers(headers)


class TestWidgetOriginFallbackIsOptInPerCaller:
    """The parameter defaults to True so every other endpoint keeps its behavior."""

    def test_default_still_accepts_a_known_widget_origin(self, auth_env: None) -> None:
        req = _FakeReq(Origin="https://geoviz.joveo.com")
        assert app.MediaPlanHandler._check_joveo_auth(req) is True

    def test_switched_off_refuses_the_same_request(self, auth_env: None) -> None:
        req = _FakeReq(Origin="https://geoviz.joveo.com")
        assert (
            app.MediaPlanHandler._check_joveo_auth(req, allow_widget_origin=False)
            is False
        )

    def test_switched_off_still_accepts_a_valid_api_key(self, auth_env: None) -> None:
        req = _FakeReq(**{"X-Nova-Api-Key": _API_KEY})
        assert (
            app.MediaPlanHandler._check_joveo_auth(req, allow_widget_origin=False)
            is True
        )

    def test_switched_off_still_accepts_a_valid_signed_cookie(
        self, auth_env: None
    ) -> None:
        req = _FakeReq(Cookie=_signed_cookie())
        assert (
            app.MediaPlanHandler._check_joveo_auth(req, allow_widget_origin=False)
            is True
        )


# ---------------------------------------------------------------------------
# 2. Authenticated: save succeeds and the list returns it
# ---------------------------------------------------------------------------


class TestSavedPlansAuthenticated:
    def test_save_then_list_and_fetch_round_trip(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        cookie = _signed_cookie()
        status, body = _request(
            live_port, "POST", "/api/saved-plans", _PLAN, cookie=cookie
        )
        assert status == 200, (status, body)
        assert body.get("saved") is True, body
        plan_id = body.get("id")
        assert isinstance(plan_id, int), body
        assert len(fake_sb.rows) == 1
        assert fake_sb.rows[0]["plan_name"] == "Hershey Q4"
        assert fake_sb.rows[0]["budget"] == 90000.0

        status, listed = _request(live_port, "GET", "/api/saved-plans", cookie=cookie)
        assert status == 200, (status, listed)
        assert [p["id"] for p in listed["plans"]] == [plan_id], listed

        status, one = _request(
            live_port, "GET", f"/api/saved-plans/{plan_id}", cookie=cookie
        )
        assert status == 200, (status, one)
        assert one["plan_data"] == {"channels": ["LinkedIn", "Indeed"]}

    def test_api_key_auth_path_can_save(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        status, body = _request(
            live_port,
            "POST",
            "/api/saved-plans",
            _PLAN,
            headers={"X-Nova-Api-Key": _API_KEY},
        )
        assert status == 200, (status, body)
        assert body.get("saved") is True

    def test_list_is_scoped_to_the_caller(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        mine = _signed_cookie("me@joveo.com")
        other = _signed_cookie("other@joveo.com")
        _request(live_port, "POST", "/api/saved-plans", _PLAN, cookie=mine)
        status, listed = _request(live_port, "GET", "/api/saved-plans", cookie=other)
        assert status == 200
        assert listed["plans"] == []


# ---------------------------------------------------------------------------
# 3. Errors are VISIBLE: a storage failure is not reported as "no saved plans"
# ---------------------------------------------------------------------------


class TestSavedPlansStorageFailuresAreVisible:
    def test_list_storage_exception_is_500_not_empty_200(
        self,
        live_port: int,
        auth_env: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import supabase_client

        monkeypatch.setattr(supabase_client, "get_client", lambda: _ExplodingSupabase())
        status, body = _request(
            live_port, "GET", "/api/saved-plans", cookie=_signed_cookie()
        )
        assert status == 500, (status, body)
        assert "plans" not in body

    def test_list_without_storage_is_503_not_empty_200(
        self,
        live_port: int,
        auth_env: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import supabase_client

        monkeypatch.setattr(supabase_client, "get_client", lambda: None)
        status, body = _request(
            live_port, "GET", "/api/saved-plans", cookie=_signed_cookie()
        )
        assert status == 503, (status, body)
        assert "plans" not in body

    def test_post_without_storage_is_503(
        self,
        live_port: int,
        auth_env: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import supabase_client

        monkeypatch.setattr(supabase_client, "get_client", lambda: None)
        status, _ = _request(
            live_port, "POST", "/api/saved-plans", _PLAN, cookie=_signed_cookie()
        )
        assert status == 503


# ---------------------------------------------------------------------------
# 4. Bug CLASS guard: no bare call to a name defined only as a class method
# ---------------------------------------------------------------------------


def _bare_calls_to_method_only_names(path: Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    bound: set[str] = set(dir(builtins))
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                bound.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                bound.update(n.id for n in ast.walk(tgt) if isinstance(n, ast.Name))
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
            bound.update(n.id for n in ast.walk(node.target) if isinstance(n, ast.Name))
        elif isinstance(node, (ast.For, ast.AsyncFor, ast.comprehension)):
            bound.update(n.id for n in ast.walk(node.target) if isinstance(n, ast.Name))
        elif isinstance(node, ast.arg):
            bound.add(node.arg)
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                if item.optional_vars is not None:
                    bound.update(
                        n.id
                        for n in ast.walk(item.optional_vars)
                        if isinstance(n, ast.Name)
                    )
        elif isinstance(node, ast.ExceptHandler) and node.name:
            bound.add(node.name)
        elif isinstance(node, ast.NamedExpr):
            bound.add(node.target.id)
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            bound.update(node.names)

    plain_funcs: set[str] = set()
    method_names: set[str] = set()

    def walk(node: ast.AST, in_class_body: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                walk(child, True)
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                (method_names if in_class_body else plain_funcs).add(child.name)
                walk(child, False)
            else:
                walk(child, in_class_body)

    walk(tree, False)
    suspicious = method_names - plain_funcs - bound
    return sorted(
        (node.lineno, node.func.id)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in suspicious
    )


_CLASS_GUARD_FILES = ["app.py"] + sorted(
    str(p.relative_to(PROJECT_ROOT)) for p in (PROJECT_ROOT / "routes").glob("*.py")
)


@pytest.mark.parametrize("rel", _CLASS_GUARD_FILES)
def test_no_bare_call_to_method_only_name(rel: str) -> None:
    hits = _bare_calls_to_method_only_names(PROJECT_ROOT / rel)
    assert (
        not hits
    ), f"{rel}: bare call(s) to a name defined only as a class method (NameError at runtime): {hits}"


def test_send_error_calls_never_pass_a_bare_status_as_the_code() -> None:
    """``_send_error(message, code: str, status: int)``: a 2-arg call with an int
    literal second argument puts the HTTP status into ``code`` and leaves status=400."""
    tree = ast.parse((PROJECT_ROOT / "app.py").read_text(encoding="utf-8"))
    bad = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_send_error"
        and len(node.args) == 2
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, int)
    ]
    assert not bad, f"_send_error(msg, <int>) at app.py lines {bad}"
