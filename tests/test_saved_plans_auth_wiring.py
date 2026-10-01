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
import base64
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
    def __init__(
        self,
        store: list[dict[str, Any]],
        ids: Iterator[int],
        log: Optional[list[tuple[str, list[tuple[str, Any]]]]] = None,
    ) -> None:
        self._store = store
        self._ids = ids
        self._log = log if log is not None else []
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
            self._log.append(("insert", [("user_email", row.get("user_email"))]))
            if "id" in row:
                if any(r["id"] == row["id"] for r in self._store):
                    raise RuntimeError(
                        'duplicate key value violates unique constraint "'
                        'nova_saved_plans_pkey" (23505)'
                    )
            else:
                row["id"] = next(self._ids)
            row["created_at"] = "2026-10-01T00:00:00Z"
            self._store.append(row)
            return _Result([row])
        self._log.append(("select", list(self._filters)))
        rows = [r for r in self._store if all(r.get(c) == v for c, v in self._filters)]
        if self._limit is not None:
            rows = rows[: self._limit]
        return _Result(rows)


class _FakeSupabase:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.log: list[tuple[str, list[tuple[str, Any]]]] = []
        self._ids = itertools.count(1)

    def table(self, name: str) -> _Query:
        assert name == "nova_saved_plans", name
        return _Query(self.rows, self._ids, self.log)


class _ExplodingSupabase:
    """Every table access raises; ``table_calls`` proves storage was reached."""

    def __init__(self) -> None:
        self.table_calls = 0

    def table(self, _name: str) -> Any:
        self.table_calls += 1
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

    def test_list_is_scoped_to_the_caller(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        """The caller's scope must be visible in the OUTGOING query.

        An empty ``plans`` list proves nothing by itself: before the NameError
        fix the list endpoint swallowed the error and returned ``200 {"plans":
        []}`` to everyone, which also "passes" an emptiness check. So seed the
        owner's row directly (independent of POST), then assert the exact
        ``user_email`` equality filter the handler sent to storage for each
        caller, and that only the owner gets the row back.
        """
        mine, other = "me@joveo.com", "other@joveo.com"
        fake_sb.rows.append(
            {
                "id": 41,
                "user_email": mine,
                "plan_name": "mine",
                "plan_data": {},
                "created_at": "2026-10-01T00:00:00Z",
            }
        )
        fake_sb.log.clear()

        status, listed = _request(
            live_port, "GET", "/api/saved-plans", cookie=_signed_cookie(other)
        )
        assert status == 200, (status, listed)
        assert listed["plans"] == []
        assert fake_sb.log == [("select", [("user_email", other)])], fake_sb.log

        fake_sb.log.clear()
        status, listed = _request(
            live_port, "GET", "/api/saved-plans", cookie=_signed_cookie(mine)
        )
        assert status == 200, (status, listed)
        assert [p["id"] for p in listed["plans"]] == [41], listed
        assert fake_sb.log == [("select", [("user_email", mine)])], fake_sb.log


# ---------------------------------------------------------------------------
# 2b. OWNERSHIP: a plan belongs to the VERIFIED user who saved it
# ---------------------------------------------------------------------------
# Independent verification (confidence 97): GET /api/saved-plans/<id> fetched by
# id only, and list/save took the owner from the UNVERIFIED nova_user_email
# cookie, so once the NameError was fixed any caller with any credential (even an
# API key alone) could read any user's plan_data by walking sequential ids, and
# with STRICT_AUTH unset a forged cookie was enough. The owner now comes only
# from _extract_authenticated_email(allow_unsigned_cookie=False) -- an
# HMAC-verified JWT or HMAC-signed session cookie -- and EVERY query is filtered
# by it. Someone else's plan is a 404 (never 403: no existence oracle).


@pytest.fixture()
def nonstrict_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """STRICT_AUTH unset: the legacy unsigned-cookie migration path is live."""
    monkeypatch.setenv("SESSION_SIGNING_SECRET", _SESSION_SECRET)
    monkeypatch.delenv("STRICT_AUTH", raising=False)
    monkeypatch.setenv("NOVA_API_KEYS", _API_KEY)
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)


_A = "alice@joveo.com"
_B = "bob@joveo.com"


def _save(port: int, cookie: str, payload: Optional[dict[str, Any]] = None) -> int:
    status, body = _request(
        port, "POST", "/api/saved-plans", payload or _PLAN, cookie=cookie
    )
    assert status == 200, (status, body)
    return int(body["id"])


class TestSavedPlansOwnership:
    def test_user_a_can_save_list_and_get_own_plan(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        a = _signed_cookie(_A)
        plan_id = _save(live_port, a)
        status, listed = _request(live_port, "GET", "/api/saved-plans", cookie=a)
        assert status == 200 and [p["id"] for p in listed["plans"]] == [plan_id]
        status, one = _request(
            live_port, "GET", f"/api/saved-plans/{plan_id}", cookie=a
        )
        assert status == 200 and one["plan_data"] == _PLAN["data"]

    def test_user_b_requesting_user_as_id_gets_404_not_the_plan(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        plan_id = _save(live_port, _signed_cookie(_A))
        status, body = _request(
            live_port, "GET", f"/api/saved-plans/{plan_id}", cookie=_signed_cookie(_B)
        )
        assert status == 404, (status, body)
        assert "plan_data" not in body and "plan_name" not in body

    def test_someone_elses_id_is_indistinguishable_from_a_missing_id(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        """No existence oracle: 404 + the same body for 'not yours' and 'not there'."""
        plan_id = _save(live_port, _signed_cookie(_A))
        b = _signed_cookie(_B)
        s1, b1 = _request(live_port, "GET", f"/api/saved-plans/{plan_id}", cookie=b)
        s2, b2 = _request(live_port, "GET", "/api/saved-plans/987654321", cookie=b)
        assert s1 == s2 == 404
        assert b1["error"] == b2["error"] and b1["code"] == b2["code"]

    def test_user_b_cannot_list_user_as_plans(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        _save(live_port, _signed_cookie(_A))
        status, listed = _request(
            live_port, "GET", "/api/saved-plans", cookie=_signed_cookie(_B)
        )
        assert status == 200 and listed["plans"] == []

    def test_user_b_cannot_overwrite_or_delete_user_as_plan(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        a, b = _signed_cookie(_A), _signed_cookie(_B)
        plan_id = _save(live_port, a)
        # POST naming A's id must create B's OWN row, never touch A's (the
        # server picks the id; a client-supplied id is ignored)
        evil = dict(_PLAN, id=plan_id, name="hijacked", data={"owned": "by-bob"})
        status, body = _request(live_port, "POST", "/api/saved-plans", evil, cookie=b)
        assert status == 200 and body["id"] != plan_id
        mine = [r for r in fake_sb.rows if r["id"] == plan_id]
        assert len(mine) == 1 and mine[0]["user_email"] == _A
        assert (
            mine[0]["plan_data"] == _PLAN["data"]
            and mine[0]["plan_name"] == _PLAN["name"]
        )
        # there is no update/delete route: the verbs must not remove or alter it
        for verb in ("DELETE", "PUT", "PATCH"):
            conn = http.client.HTTPConnection("127.0.0.1", live_port, timeout=30)
            token = app._generate_csrf_token()
            conn.request(
                verb,
                f"/api/saved-plans/{plan_id}",
                body=b"{}",
                headers={
                    "Cookie": f"{b}; csrf_token={token}",
                    "X-CSRF-Token": token,
                    "Content-Type": "application/json",
                    "X-Forwarded-For": _fresh_ip(),
                },
            )
            conn.getresponse().read()
            conn.close()
        assert [r["plan_data"] for r in fake_sb.rows if r["id"] == plan_id] == [
            _PLAN["data"]
        ]

    def test_every_query_is_filtered_by_the_verified_owner(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        a = _signed_cookie(_A)
        plan_id = _save(live_port, a)
        fake_sb.log.clear()
        _request(live_port, "GET", "/api/saved-plans", cookie=a)
        _request(live_port, "GET", f"/api/saved-plans/{plan_id}", cookie=a)
        selects = [f for op, f in fake_sb.log if op == "select"]
        assert len(selects) == 2
        for filters in selects:
            assert ("user_email", _A) in filters, filters
        assert ("id", plan_id) in selects[1]

    def test_saved_row_stores_the_verified_email_not_the_raw_cookie_value(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        _save(live_port, _signed_cookie(_A))
        assert (
            fake_sb.rows[0]["user_email"] == _A
        ), "no '|signature' suffix, no 'unknown'"

    def test_new_plan_ids_are_random_and_fit_a_js_number(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        a = _signed_cookie(_A)
        ids = [_save(live_port, a) for _ in range(3)]
        assert len(set(ids)) == 3
        assert all(1 << 8 < i < (1 << 53) for i in ids), ids  # JS-exact, not 1,2,3
        assert sorted(ids) != list(range(min(ids), min(ids) + 3)), "not sequential"

    def test_an_old_sequential_id_stays_readable_only_by_its_owner(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        fake_sb.rows.append(
            {"id": 7, "user_email": _A, "plan_name": "old", "plan_data": {"k": 1}}
        )
        s_a, body = _request(
            live_port, "GET", "/api/saved-plans/7", cookie=_signed_cookie(_A)
        )
        s_b, _ = _request(
            live_port, "GET", "/api/saved-plans/7", cookie=_signed_cookie(_B)
        )
        assert (s_a, body["plan_data"]) == (200, {"k": 1}) and s_b == 404

    def test_id_collision_retries_with_a_fresh_id(
        self,
        live_port: int,
        auth_env: None,
        fake_sb: _FakeSupabase,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_sb.rows.append({"id": 5, "user_email": _B, "plan_data": {}})
        picks = iter([5, 5, 4242])
        monkeypatch.setattr(app.secrets, "randbits", lambda _n: next(picks))
        assert _save(live_port, _signed_cookie(_A)) == 4242

    def test_storage_exception_text_is_not_leaked_to_the_client(
        self,
        live_port: int,
        auth_env: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The 500 body is a fixed message with no exception text.

        Asserting only that the storage error text is absent is vacuous: before
        the NameError fix the handler never reached storage and answered
        ``Failed to save plan: name '_check_joveo_auth' is not defined`` -- a
        leak of a different exception that such a check cannot see. Pin the exact
        body, and prove the storage layer was actually reached.
        """
        import supabase_client

        boom = _ExplodingSupabase()
        monkeypatch.setattr(supabase_client, "get_client", lambda: boom)
        status, body = _request(
            live_port, "POST", "/api/saved-plans", _PLAN, cookie=_signed_cookie(_A)
        )
        assert status == 500, (status, body)
        assert body["error"] == "Failed to save plan", body
        assert "supabase is down" not in json.dumps(body)
        assert boom.table_calls >= 1, "the save never reached the storage layer"


class TestOwnerMustBeAVerifiedIdentity:
    """The owner is never taken from a header/cookie the client can type."""

    _ROUTES = (
        ("GET", "/api/saved-plans", None),
        ("GET", "/api/saved-plans/7", None),
        ("POST", "/api/saved-plans", _PLAN),
    )

    def _seed(self, fake_sb: _FakeSupabase) -> None:
        fake_sb.rows.append(
            {"id": 7, "user_email": _A, "plan_name": "a", "plan_data": {"s": 1}}
        )

    def test_forged_unsigned_cookie_is_refused_403_while_strict_auth_is_off(
        self, live_port: int, nonstrict_env: None, fake_sb: _FakeSupabase
    ) -> None:
        """STRICT_AUTH unset lets ``_check_joveo_auth`` accept ``nova_user_email=
        victim@joveo.com`` (legacy migration). It must still not become an OWNER."""
        self._seed(fake_sb)
        for method, path, payload in self._ROUTES:
            status, body = _request(
                live_port,
                method,
                path,
                payload,
                cookie=f"nova_user_email={_A}",
            )
            assert status == 403, (method, path, status, body)
            assert body.get("code") == "AUTH_REQUIRED"
            assert "plan_data" not in body and "plans" not in body
        assert len(fake_sb.rows) == 1, "nothing written by the forger"

    def test_forged_unsigned_cookie_is_refused_401_under_strict_auth(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        self._seed(fake_sb)
        for method, path, payload in self._ROUTES:
            status, _ = _request(
                live_port, method, path, payload, cookie=f"nova_user_email={_A}"
            )
            assert status == 401, (method, path, status)

    @pytest.mark.parametrize("strict", [True, False])
    def test_cookie_with_a_wrong_signature_is_refused(
        self,
        live_port: int,
        fake_sb: _FakeSupabase,
        monkeypatch: pytest.MonkeyPatch,
        strict: bool,
    ) -> None:
        monkeypatch.setenv("SESSION_SIGNING_SECRET", _SESSION_SECRET)
        monkeypatch.setenv("NOVA_API_KEYS", _API_KEY)
        if strict:
            monkeypatch.setenv("STRICT_AUTH", "1")
        else:
            monkeypatch.delenv("STRICT_AUTH", raising=False)
        self._seed(fake_sb)
        forged = f"nova_user_email={_A}|{'0' * 64}"
        for method, path, payload in self._ROUTES:
            status, body = _request(live_port, method, path, payload, cookie=forged)
            assert status in (401, 403), (method, path, status, body)
            assert "plan_data" not in body and "plans" not in body
        assert len(fake_sb.rows) == 1

    def test_api_key_without_a_verified_user_is_refused_for_per_user_endpoints(
        self, live_port: int, nonstrict_env: None, fake_sb: _FakeSupabase
    ) -> None:
        """An API key proves a caller, not WHICH user: refused (403), reads and writes."""
        self._seed(fake_sb)
        for method, path, payload in self._ROUTES:
            status, body = _request(
                live_port,
                method,
                path,
                payload,
                headers={"X-Nova-Api-Key": _API_KEY},
            )
            assert status == 403, (method, path, status, body)
            assert body.get("code") == "AUTH_REQUIRED"
            assert "plan_data" not in body and "plans" not in body
        assert len(fake_sb.rows) == 1

    def test_api_key_plus_forged_cookie_is_still_refused(
        self, live_port: int, nonstrict_env: None, fake_sb: _FakeSupabase
    ) -> None:
        self._seed(fake_sb)
        for method, path, payload in self._ROUTES:
            status, _ = _request(
                live_port,
                method,
                path,
                payload,
                cookie=f"nova_user_email={_A}",
                headers={"X-Nova-Api-Key": _API_KEY},
            )
            assert status == 403, (method, path, status)

    def test_a_non_joveo_signed_cookie_is_refused(
        self, live_port: int, nonstrict_env: None, fake_sb: _FakeSupabase
    ) -> None:
        status, _ = _request(
            live_port,
            "POST",
            "/api/saved-plans",
            _PLAN,
            cookie=_signed_cookie("mallory@example.com"),
        )
        assert status in (401, 403) and fake_sb.rows == []

    def test_a_valid_signed_cookie_works_with_strict_auth_off_too(
        self, live_port: int, nonstrict_env: None, fake_sb: _FakeSupabase
    ) -> None:
        a = _signed_cookie(_A)
        plan_id = _save(live_port, a)
        status, _ = _request(live_port, "GET", f"/api/saved-plans/{plan_id}", cookie=a)
        assert status == 200

    def test_verified_supabase_jwt_identifies_the_owner(
        self,
        live_port: int,
        fake_sb: _FakeSupabase,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        secret = "unit-test-supabase-jwt-secret"
        monkeypatch.setenv("SUPABASE_JWT_SECRET", secret)
        monkeypatch.setenv("SESSION_SIGNING_SECRET", _SESSION_SECRET)
        monkeypatch.delenv("NOVA_API_KEYS", raising=False)
        bearer = {"Authorization": f"Bearer {_jwt(_A, secret)}"}
        status, body = _request(
            live_port, "POST", "/api/saved-plans", _PLAN, headers=bearer
        )
        assert status == 200, (status, body)
        assert fake_sb.rows[0]["user_email"] == _A
        status, listed = _request(live_port, "GET", "/api/saved-plans", headers=bearer)
        assert status == 200 and len(listed["plans"]) == 1
        other = {"Authorization": f"Bearer {_jwt(_B, secret)}"}
        status, _ = _request(
            live_port, "GET", f"/api/saved-plans/{body['id']}", headers=other
        )
        assert status == 404

    def test_a_jwt_signed_with_the_wrong_secret_is_refused(
        self,
        live_port: int,
        fake_sb: _FakeSupabase,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "the-real-secret")
        monkeypatch.delenv("NOVA_API_KEYS", raising=False)
        monkeypatch.delenv("STRICT_AUTH", raising=False)
        forged = {"Authorization": f"Bearer {_jwt(_A, 'attacker-guess')}"}
        for method, path, payload in self._ROUTES:
            status, _ = _request(live_port, method, path, payload, headers=forged)
            assert status in (401, 403), (method, path, status)
        assert fake_sb.rows == []


def _jwt(email: str, secret: str) -> str:
    def b64(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    head = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = b64(json.dumps({"email": email, "exp": int(time.time()) + 3600}).encode())
    sig = hmac.new(
        secret.encode(), f"{head}.{payload}".encode("ascii"), hashlib.sha256
    ).digest()
    return f"{head}.{payload}.{b64(sig)}"


class TestVerifiedIdentityHelper:
    """_extract_authenticated_email: the strict switch only removes the unsigned path."""

    def test_default_keeps_the_legacy_unsigned_cookie_for_rate_limit_keying(
        self, nonstrict_env: None
    ) -> None:
        req = _FakeReq(Cookie=f"nova_user_email={_A}")
        assert app.MediaPlanHandler._extract_authenticated_email(req) == _A

    def test_strict_mode_ignores_the_unsigned_cookie(self, nonstrict_env: None) -> None:
        req = _FakeReq(Cookie=f"nova_user_email={_A}")
        assert (
            app.MediaPlanHandler._extract_authenticated_email(
                req, allow_unsigned_cookie=False
            )
            == ""
        )

    def test_strict_mode_still_returns_a_signed_cookie_identity(
        self, nonstrict_env: None
    ) -> None:
        req = _FakeReq(Cookie=_signed_cookie(_A))
        assert (
            app.MediaPlanHandler._extract_authenticated_email(
                req, allow_unsigned_cookie=False
            )
            == _A
        )


# ---------------------------------------------------------------------------
# 2c. Plan-id validation: ASCII digits only, bounded; anything else is a 404
# ---------------------------------------------------------------------------
# ``"²".isdigit()`` is True but ``int("²")`` raises ValueError, so the old
# ``plan_id.isdigit()`` guard let a superscript digit through to ``int()`` and
# the request ended in a 500. (``str.isdigit`` also accepts Arabic-Indic and
# fullwidth digits, and it is unbounded.) Ids now go through
# ``re.fullmatch(r"[0-9]{1,16}", ...)``, and a non-matching id is answered with
# the SAME 404 as a missing one -- no 400/500 side channel.

_BAD_IDS: list[str] = [
    "²",  # superscript two: isdigit() True, int() raises
    "³",  # superscript three
    "¹",  # superscript one
    "٣",  # ARABIC-INDIC DIGIT THREE
    "٣٤",  # two Arabic-Indic digits
    "３",  # FULLWIDTH DIGIT THREE
    "１２",  # two fullwidth digits
    "1" * 17,  # one digit past the bound
    "",  # empty
    "-5",  # negative
    "+5",  # explicit sign
    " 7",  # whitespace
    "7 ",
    "7\n",  # fullmatch must not tolerate a trailing newline
    "\t7",
    "7.0",
    "0x1f",
    "abc",
]


class TestSavedPlanIdValidation:
    @pytest.mark.parametrize("raw", ["7", "0", "42", "1234567890", "9" * 16])
    def test_helper_accepts_plain_ascii_digit_ids(self, raw: str) -> None:
        assert app._parse_saved_plan_id(raw) == int(raw)

    @pytest.mark.parametrize("raw", _BAD_IDS)
    def test_helper_rejects_everything_else_without_raising(self, raw: str) -> None:
        assert app._parse_saved_plan_id(raw) is None

    def test_a_missing_id_is_a_404_with_a_fixed_body(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        status, body = _request(
            live_port,
            "GET",
            "/api/saved-plans/987654321",
            cookie=_signed_cookie(_A),
        )
        assert status == 404, (status, body)
        assert body["error"] == "Plan not found"

    def test_superscript_two_on_the_wire_is_the_same_404_not_a_500(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        """BaseHTTPRequestHandler decodes the request line as ISO-8859-1, so the
        single byte 0xB2 arrives as U+00B2 -- the verifier's repro."""
        cookie = _signed_cookie(_A)
        missing = _request(
            live_port, "GET", "/api/saved-plans/987654321", cookie=cookie
        )
        sock = socket.create_connection(("127.0.0.1", live_port), timeout=30)
        try:
            sock.sendall(
                b"GET /api/saved-plans/\xb2 HTTP/1.1\r\n"
                b"Host: 127.0.0.1\r\n"
                + f"X-Forwarded-For: {_fresh_ip()}\r\n".encode("ascii")
                + f"Cookie: {cookie}\r\n".encode("ascii")
                + b"Connection: close\r\n\r\n"
            )
            chunks: list[bytes] = []
            while True:
                data = sock.recv(65536)
                if not data:
                    break
                chunks.append(data)
        finally:
            sock.close()
        head, _, raw_body = b"".join(chunks).partition(b"\r\n\r\n")
        status = int(head.split(b" ", 2)[1])
        assert status == 404, (head[:200], raw_body[:200])
        assert (status, json.loads(raw_body)) == missing

    @pytest.mark.parametrize(
        "segment",
        [
            "%C2%B2",  # what a browser sends for U+00B2 (UTF-8, percent-encoded)
            "%D9%A3",  # percent-encoded Arabic-Indic digit three
            "%EF%BC%93",  # percent-encoded fullwidth digit three
            "12345678901234567",  # 17 digits
            "-5",
            "%207",  # encoded leading space
            "7%20",
            "abc",
        ],
    )
    def test_bad_ids_over_http_are_the_same_404_as_a_missing_id(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase, segment: str
    ) -> None:
        cookie = _signed_cookie(_A)
        fake_sb.rows.append({"id": 7, "user_email": _A, "plan_data": {"k": 1}})
        missing = _request(
            live_port, "GET", "/api/saved-plans/987654321", cookie=cookie
        )
        got = _request(live_port, "GET", f"/api/saved-plans/{segment}", cookie=cookie)
        assert got == missing, (segment, got, missing)
        assert got[0] == 404

    def test_an_empty_id_is_a_404_and_leaks_nothing(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        fake_sb.rows.append({"id": 7, "user_email": _A, "plan_data": {"k": 1}})
        status, body = _request(
            live_port, "GET", "/api/saved-plans/", cookie=_signed_cookie(_A)
        )
        assert status == 404, (status, body)
        assert "plan_data" not in body and "plans" not in body

    def test_an_invalid_id_does_not_touch_storage(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        fake_sb.log.clear()
        status, _ = _request(
            live_port,
            "GET",
            "/api/saved-plans/12345678901234567",
            cookie=_signed_cookie(_A),
        )
        assert status == 404
        assert fake_sb.log == [], "an id that cannot exist must not hit the database"

    def test_a_valid_id_still_resolves_for_its_owner(
        self, live_port: int, auth_env: None, fake_sb: _FakeSupabase
    ) -> None:
        fake_sb.rows.append({"id": 7, "user_email": _A, "plan_data": {"k": 1}})
        status, body = _request(
            live_port, "GET", "/api/saved-plans/7", cookie=_signed_cookie(_A)
        )
        assert (status, body["plan_data"]) == (200, {"k": 1})


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
