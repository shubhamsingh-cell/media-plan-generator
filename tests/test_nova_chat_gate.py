"""NOVA_CHAT_GATE: signed widget tokens for guest chat turns.

Under ``off`` (the default) /api/chat, /api/chat/stream and /ws/chat admit a
same-origin guest exactly as before. Under ``signed`` a guest turn also needs
a widget token minted by POST /api/chat/widget-token, bound to the turn's
conversation id and unexpired; a matching Origin alone gets 401. The admin
key and real credentials are untouched.

Live tests run a real app.ThreadedHTTPServer on an ephemeral port (the
fixture pattern of tests/test_api_chat_origin.py). Requests that pass the
gate are built to stop at the next validation step (a non-string message ->
400) so no LLM call is ever made.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import socket
import struct
import sys
import threading
import time
from pathlib import Path
from typing import Iterator, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402
import nova_chat_gate as gate  # noqa: E402

CID = "nova-11111111-2222-4333-8444-555555555555"
OTHER_CID = "nova-99999999-8888-4777-8666-555555555555"
LOCAL_ORIGIN = "http://localhost:5000"


# ---------------------------------------------------------------------------
# Unit: token format, expiry, binding, mode parsing, secret source
# ---------------------------------------------------------------------------


class TestTokenUnit:
    def test_minted_token_verifies_for_its_conversation(self) -> None:
        assert gate.check(gate.mint(CID), CID) == "ok"

    def test_token_expires_after_ttl(self) -> None:
        t0 = 1_800_000_000
        token = gate.mint(CID, now=t0)
        assert gate.check(token, CID, now=t0 + gate.TOKEN_TTL_SECONDS - 1) == "ok"
        assert gate.check(token, CID, now=t0 + gate.TOKEN_TTL_SECONDS) == "expired"

    def test_wrong_conversation_binding_rejected(self) -> None:
        assert gate.check(gate.mint(CID), OTHER_CID) == "wrong_conversation"

    def test_tampered_binding_or_expiry_fails_signature(self) -> None:
        v, _cid, exp, sig = gate.mint(CID).split(".")
        swapped = ".".join((v, gate._b64(OTHER_CID.encode()), exp, sig))
        assert gate.check(swapped, OTHER_CID) == "bad_signature"
        extended = ".".join((v, _cid, str(int(exp) + 86400), sig))
        assert gate.check(extended, CID) == "bad_signature"

    @pytest.mark.parametrize("bad", ["", None, 42, "v1.x", "v2.a.1.b", "v1.a.notnum.b"])
    def test_missing_or_malformed(self, bad: object) -> None:
        assert gate.check(bad, CID) in ("missing", "malformed")

    def test_signing_key_change_invalidates_tokens(self, monkeypatch) -> None:
        monkeypatch.setenv(gate.SECRET_ENV, "first-secret-for-test")
        token = gate.mint(CID)
        monkeypatch.setenv(gate.SECRET_ENV, "second-secret-for-test")
        assert gate.check(token, CID) == "bad_signature"

    def test_secret_source_order(self, monkeypatch) -> None:
        monkeypatch.delenv(gate.SECRET_ENV, raising=False)
        monkeypatch.delenv("SESSION_SIGNING_SECRET", raising=False)
        assert gate.secret_source().startswith("boot-random")
        monkeypatch.setenv("SESSION_SIGNING_SECRET", "s")
        assert gate.secret_source().startswith("SESSION_SIGNING_SECRET")
        monkeypatch.setenv(gate.SECRET_ENV, "w")
        assert gate.secret_source() == gate.SECRET_ENV

    @pytest.mark.parametrize(
        "raw,mode",
        [("", "off"), ("off", "off"), ("signed", "signed"), (" SIGNED ", "signed"), ("sign", "off")],
    )
    def test_mode_parsing(self, monkeypatch, raw: str, mode: str) -> None:
        monkeypatch.setenv(gate.GATE_ENV, raw)
        assert gate.gate_mode() == mode

    def test_response_fields_only_under_signed(self, monkeypatch) -> None:
        monkeypatch.delenv(gate.GATE_ENV, raising=False)
        assert gate.response_fields(CID) == {}
        monkeypatch.setenv(gate.GATE_ENV, "signed")
        fields = gate.response_fields(CID)
        assert gate.check(fields[gate.WIDGET_TOKEN_FIELD], CID) == "ok"


# ---------------------------------------------------------------------------
# Live HTTP + WebSocket
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def server_port() -> Iterator[int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    yield port
    server.shutdown()
    server.server_close()


@pytest.fixture
def signed(monkeypatch):
    monkeypatch.setenv(gate.GATE_ENV, "signed")
    # Fresh per-IP buckets so a long run never trips the chat rate limit.
    monkeypatch.setattr(app, "_rl_chat", app.RateLimiter())
    monkeypatch.setattr(app, "_rl_general", app.RateLimiter())


@pytest.fixture
def gate_off(monkeypatch):
    monkeypatch.delenv(gate.GATE_ENV, raising=False)
    monkeypatch.setattr(app, "_rl_chat", app.RateLimiter())
    monkeypatch.setattr(app, "_rl_general", app.RateLimiter())


def _post(port: int, path: str, payload: Optional[object], headers: dict):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    body = b"" if payload is None else json.dumps(payload).encode()
    hdrs = {"Content-Type": "application/json", **headers}
    conn.request("POST", path, body=body, headers=hdrs)
    resp = conn.getresponse()
    raw = resp.read()
    try:
        data = json.loads(raw) if raw else {}
    except ValueError:
        data = {"_raw": raw.decode("utf-8", "replace")}
    return resp.status, data


def _code(data: dict) -> str:
    err = data.get("error")
    if isinstance(err, dict):
        return err.get("code") or ""
    return data.get("code") or ""


# A body that passes the gate and then fails type validation (400) before any
# LLM work: the gate verdict is observable without spending anything.
def _stop_body(cid: str = CID) -> dict:
    return {"conversation_id": cid, "message": 123}


CHAT_PATHS = ["/api/chat", "/api/chat/stream"]


class TestGateOff:
    @pytest.mark.parametrize("path", CHAT_PATHS)
    def test_same_origin_guest_admitted_without_token(self, server_port, gate_off, path):
        status, _ = _post(server_port, path, _stop_body(), {"Origin": LOCAL_ORIGIN})
        assert status == 400  # past the gate, stopped by type validation

    @pytest.mark.parametrize("path", CHAT_PATHS)
    def test_empty_body_still_400_not_401(self, server_port, gate_off, path):
        status, _ = _post(server_port, path, None, {"Origin": LOCAL_ORIGIN})
        assert status == 400

    def test_mint_reports_off_and_mints_nothing(self, server_port, gate_off):
        status, data = _post(
            server_port, gate.MINT_PATH, {"conversation_id": CID}, {"Origin": LOCAL_ORIGIN}
        )
        assert status == 200
        assert data == {"gate": "off"}


class TestGateSigned:
    @pytest.mark.parametrize("path", CHAT_PATHS)
    def test_matching_origin_without_token_401(self, server_port, signed, path):
        status, data = _post(server_port, path, _stop_body(), {"Origin": LOCAL_ORIGIN})
        assert status == 401
        assert _code(data) == gate.TOKEN_REQUIRED_CODE

    @pytest.mark.parametrize("path", CHAT_PATHS)
    def test_spoofed_production_origin_without_token_401(self, server_port, signed, path):
        """The bypass the gate exists for: a non-browser caller setting the
        production Origin header."""
        status, _ = _post(
            server_port,
            path,
            _stop_body(),
            {"Origin": "https://media-plan-generator.onrender.com"},
        )
        assert status == 401

    @pytest.mark.parametrize("path", CHAT_PATHS)
    def test_valid_token_passes_gate(self, server_port, signed, path):
        status, _ = _post(
            server_port,
            path,
            _stop_body(),
            {"Origin": LOCAL_ORIGIN, gate.WIDGET_TOKEN_HEADER: gate.mint(CID)},
        )
        assert status == 400  # past the gate

    @pytest.mark.parametrize("path", CHAT_PATHS)
    def test_token_for_other_conversation_401(self, server_port, signed, path):
        status, _ = _post(
            server_port,
            path,
            _stop_body(OTHER_CID),
            {"Origin": LOCAL_ORIGIN, gate.WIDGET_TOKEN_HEADER: gate.mint(CID)},
        )
        assert status == 401

    def test_expired_token_401(self, server_port, signed):
        stale = gate.mint(CID, now=time.time() - gate.TOKEN_TTL_SECONDS - 5)
        status, _ = _post(
            server_port,
            "/api/chat",
            _stop_body(),
            {"Origin": LOCAL_ORIGIN, gate.WIDGET_TOKEN_HEADER: stale},
        )
        assert status == 401

    def test_unsigned_cookie_and_widget_domain_do_not_bypass(self, server_port, signed):
        status, _ = _post(
            server_port,
            "/api/chat",
            _stop_body(),
            {
                "Origin": "https://geoviz.joveo.com",
                "Cookie": "nova_user_email=someone@joveo.com",
            },
        )
        assert status == 401

    def test_admin_key_untouched(self, server_port, signed, monkeypatch):
        monkeypatch.setattr(app, "ADMIN_API_KEY", "test-only-admin-key-gate")
        status, _ = _post(
            server_port,
            "/api/chat",
            _stop_body(),
            {"Origin": "https://evil.com", "X-Admin-Key": "test-only-admin-key-gate"},
        )
        assert status == 400  # admitted without any widget token

    def test_mint_then_chat(self, server_port, signed):
        status, data = _post(
            server_port, gate.MINT_PATH, {"conversation_id": CID}, {"Origin": LOCAL_ORIGIN}
        )
        assert status == 200 and data["gate"] == "signed"
        assert data["expires_in"] == gate.TOKEN_TTL_SECONDS
        status, _ = _post(
            server_port,
            "/api/chat/stream",
            _stop_body(),
            {"Origin": LOCAL_ORIGIN, gate.WIDGET_TOKEN_HEADER: data["widget_token"]},
        )
        assert status == 400

    def test_mint_rejects_foreign_origin(self, server_port, signed):
        status, _ = _post(
            server_port, gate.MINT_PATH, {"conversation_id": CID}, {"Origin": "https://evil.com"}
        )
        assert status == 401

    @pytest.mark.parametrize("cid", ["", "x" * 200, "has space", None])
    def test_mint_rejects_bad_conversation_id(self, server_port, signed, cid):
        status, _ = _post(
            server_port, gate.MINT_PATH, {"conversation_id": cid}, {"Origin": LOCAL_ORIGIN}
        )
        assert status == 400


# -- WebSocket: the token rides in each chat message --------------------------


def _ws_connect(port: int, origin: str = LOCAL_ORIGIN) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall(
        (
            "GET /ws/chat HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{port}\r\n"
            "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
            f"Origin: {origin}\r\n\r\n"
        ).encode()
    )
    head = b""
    while b"\r\n\r\n" not in head:
        chunk = sock.recv(1)
        if not chunk:
            break
        head += chunk
    assert b" 101 " in head.split(b"\r\n", 1)[0], head
    return sock


def _ws_send(sock: socket.socket, obj: dict) -> None:
    payload = json.dumps(obj).encode()
    mask = os.urandom(4)
    header = bytes([0x81])
    n = len(payload)
    if n < 126:
        header += bytes([0x80 | n])
    else:
        header += bytes([0x80 | 126]) + struct.pack(">H", n)
    sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))


def _ws_recv(sock: socket.socket) -> dict:
    def exact(n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("closed")
            buf += chunk
        return buf

    b1, b2 = exact(2)
    n = b2 & 0x7F
    if n == 126:
        n = struct.unpack(">H", exact(2))[0]
    elif n == 127:
        n = struct.unpack(">Q", exact(8))[0]
    return json.loads(exact(n))


class TestWebSocketGate:
    def test_signed_message_without_token_gets_401_frame(self, server_port, signed):
        sock = _ws_connect(server_port)
        try:
            _ws_send(sock, {"type": "chat", "message": "hi", "conversation_id": CID})
            frame = _ws_recv(sock)
        finally:
            sock.close()
        assert frame["code"] == gate.TOKEN_REQUIRED_CODE
        assert frame["done"] is True
        assert frame["error"].startswith("HTTP 401")

    def test_signed_message_with_wrong_binding_rejected(self, server_port, signed):
        sock = _ws_connect(server_port)
        try:
            _ws_send(
                sock,
                {
                    "type": "chat",
                    "message": "hi",
                    "conversation_id": OTHER_CID,
                    "widget_token": gate.mint(CID),
                },
            )
            frame = _ws_recv(sock)
        finally:
            sock.close()
        assert frame["code"] == gate.TOKEN_REQUIRED_CODE

    def test_signed_valid_token_reaches_validation(self, server_port, signed):
        """A valid token passes; the empty message then fails validation, so
        nothing is generated."""
        sock = _ws_connect(server_port)
        try:
            _ws_send(
                sock,
                {
                    "type": "chat",
                    "message": "",
                    "conversation_id": CID,
                    "widget_token": gate.mint(CID),
                },
            )
            frame = _ws_recv(sock)
        finally:
            sock.close()
        assert frame.get("code") != gate.TOKEN_REQUIRED_CODE
        assert frame["error"] == "Empty message"

    def test_off_message_without_token_reaches_validation(self, server_port, gate_off):
        sock = _ws_connect(server_port)
        try:
            _ws_send(sock, {"type": "chat", "message": "", "conversation_id": CID})
            frame = _ws_recv(sock)
        finally:
            sock.close()
        assert frame == {"error": "Empty message", "done": True}


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))


# ---------------------------------------------------------------------------
# Review 2026-11-03 (test adequacy): wrong gates that the cases above let
# through -- a check() accepting any opaque string, a gate that locks out real
# credentials, and replies that stop refreshing the token.
# ---------------------------------------------------------------------------


class TestGateAdequacy:
    @pytest.mark.parametrize(
        "forged",
        [
            "a" * 64,
            "0123456789abcdef" * 4,
            "v1." + gate._b64(CID.encode()) + ".9999999999." + "ab" * 32,
        ],
    )
    def test_forged_tokens_401(self, server_port, signed, forged):
        status, data = _post(
            server_port,
            "/api/chat",
            _stop_body(),
            {"Origin": LOCAL_ORIGIN, gate.WIDGET_TOKEN_HEADER: forged},
        )
        assert status == 401 and _code(data) == gate.TOKEN_REQUIRED_CODE

    def test_signed_session_cookie_admitted_without_token(self, server_port, signed, monkeypatch):
        import hashlib
        import hmac as _hmac

        monkeypatch.setenv("SESSION_SIGNING_SECRET", "test-only-session-secret")
        email = "someone@joveo.com"
        sig = _hmac.new(b"test-only-session-secret", email.encode(), hashlib.sha256).hexdigest()
        status, _ = _post(
            server_port,
            "/api/chat",
            _stop_body(),
            {"Origin": "https://evil.com", "Cookie": f"nova_user_email={email}|{sig}"},
        )
        assert status == 400  # admitted, then stopped by validation

    def test_nova_api_key_admitted_without_token(self, server_port, signed, monkeypatch):
        monkeypatch.setenv("NOVA_API_KEYS", "test-only-widget-key")
        status, _ = _post(
            server_port,
            "/api/chat/stream",
            _stop_body(),
            {"X-Nova-Api-Key": "test-only-widget-key"},
        )
        assert status == 400

    def test_successful_turn_returns_a_fresh_token_for_its_conversation(self, server_port, signed):
        """A greeting is answered by the rule-based path (no model call)."""
        status, data = _post(
            server_port,
            "/api/chat",
            {"conversation_id": CID, "message": "hi"},
            {"Origin": LOCAL_ORIGIN, gate.WIDGET_TOKEN_HEADER: gate.mint(CID)},
        )
        assert status == 200, data
        fresh = data.get(gate.WIDGET_TOKEN_FIELD)
        assert fresh and gate.check(fresh, data["conversation_id"]) == "ok"

    @pytest.mark.parametrize("exp", ["²", "1" * 4300, "12345678901234"])
    def test_non_ascii_or_huge_expiry_is_malformed_not_500(self, server_port, signed, exp):
        token = "v1." + gate._b64(CID.encode()) + "." + exp + "." + "ab" * 32
        assert gate.check(token, CID) == "malformed"
        status, _ = _post(
            server_port,
            "/api/chat",
            _stop_body(),
            {"Origin": LOCAL_ORIGIN, gate.WIDGET_TOKEN_HEADER: token.encode("utf-8").decode("latin-1")},
        )
        assert status == 401
