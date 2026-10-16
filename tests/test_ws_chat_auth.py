"""WS /ws/chat accepts the same credentials as POST /api/chat/stream.

Pre-fix the handshake required ``_check_joveo_auth()`` alone, so a
same-origin guest (the wizard's chat widget) got a 401 on all three upgrade
attempts before nova-chat.js fell back to SSE -- a wasted handshake and a
WARNING per chat open (prod logs 2026-10-16: 12 x 401). Browsers cannot set
headers on a WebSocket, so Origin is the only credential a guest has. The
SSE endpoint already accepts ``_check_same_origin_auth()``; the WS gate now
accepts the same pair. The Origin allowlist after it is unchanged.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import struct

import pytest

import app as app_module
from tests.live_server import live_port  # noqa: F401 (fixture)


def _upgrade(port: int, origin: "str | None", extra: "list[str] | None" = None) -> int:
    """Raw WS upgrade; returns the HTTP status code the server answered."""
    app_module._rate_limit_store.clear()
    lines = [
        "GET /ws/chat HTTP/1.1",
        f"Host: 127.0.0.1:{port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        "Sec-WebSocket-Key: " + base64.b64encode(os.urandom(16)).decode("ascii"),
        "Sec-WebSocket-Version: 13",
    ]
    if origin is not None:
        lines.append(f"Origin: {origin}")
    lines.extend(extra or [])
    request = ("\r\n".join(lines) + "\r\n\r\n").encode("ascii")
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(request)
        data = b""
        while b"\r\n" not in data:
            chunk = sock.recv(1024)
            if not chunk:
                break
            data += chunk
    return int(data.split(b"\r\n", 1)[0].split()[1])


def test_same_origin_guest_is_not_401(live_port: int) -> None:
    status = _upgrade(live_port, "http://localhost:5000")
    assert status != 401
    assert status == 101


def test_foreign_origin_is_still_rejected(live_port: int) -> None:
    status = _upgrade(live_port, "https://evil.example.com")
    assert status == 401


def test_no_origin_no_cookie_is_still_401(live_port: int) -> None:
    status = _upgrade(live_port, None)
    assert status == 401


def test_origin_allowlist_still_applies_after_auth(live_port: int) -> None:
    # localhost passes the same-origin host check on ANY port, but only the
    # allowlisted dev ports may open a WebSocket: the CSWSH gate stays.
    status = _upgrade(live_port, "http://localhost:3000")
    assert status == 403


def test_referer_only_guest_is_401(live_port: int) -> None:
    # Browsers always send Origin on an upgrade. A Referer-only client passes
    # the HTTP same-origin check but must not get a WS (empty Origin would
    # also skip the allowlist).
    status = _upgrade(live_port, None, extra=["Referer: http://localhost:5000/"])
    assert status == 401


# ── per-message rate limits ──────────────────────────────────────────────


def _open(port: int) -> socket.socket:
    app_module._rate_limit_store.clear()
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    req = (
        "GET /ws/chat HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n"
        "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
        f"Sec-WebSocket-Key: {key}\r\nOrigin: http://localhost:5000\r\n\r\n"
    )
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    sock.sendall(req.encode("ascii"))
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(1024)
        assert chunk, "server closed during handshake"
        buf += chunk
    assert buf.split()[1] == b"101"
    return sock


def _send(sock: socket.socket, obj: dict) -> None:
    payload = json.dumps(obj).encode("utf-8")
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    assert len(payload) < 126
    sock.sendall(bytes([0x81, 0x80 | len(payload)]) + mask + masked)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


def _frame(sock: socket.socket) -> dict:
    head = _recv_exact(sock, 2)
    length = head[1] & 0x7F
    if length == 126:
        length = struct.unpack(">H", _recv_exact(sock, 2))[0]
    payload = _recv_exact(sock, length)
    if head[0] & 0x0F == 0x8:  # close frame
        raise EOFError
    return json.loads(payload)


def _frames_until(sock: socket.socket, pred) -> list:
    out = []
    while True:
        f = _frame(sock)
        out.append(f)
        if pred(f):
            return out


@pytest.fixture
def llm_calls(monkeypatch: pytest.MonkeyPatch) -> list:
    """Stub the LLM chat stream; every call is recorded."""
    import nova

    calls: list = []

    def fake_stream(data, cancel_event=None):
        calls.append(data.get("message"))
        yield {"done": True, "full_response": "ok"}

    monkeypatch.setattr(nova, "handle_chat_request_stream", fake_stream)
    monkeypatch.setattr(app_module, "_enrich_chat_context", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "_generate_followup_questions", lambda *a: [])
    return calls


_MSG = {"type": "chat", "message": "hello", "conversation_id": "ws-rate-test"}


def _is_error(f: dict) -> bool:
    return f.get("code") == "rate_limited"


def test_guest_over_per_ip_limit_gets_error_frame_and_no_llm_call(
    live_port: int, llm_calls: list, monkeypatch: pytest.MonkeyPatch
) -> None:
    handler = app_module.MediaPlanHandler
    seen = {"n": 0}

    def per_ip(self) -> bool:  # call 1 is the handshake; every later one is over limit
        seen["n"] += 1
        return seen["n"] == 1

    monkeypatch.setattr(handler, "_check_rate_limit", per_ip)
    sock = _open(live_port)
    try:
        _send(sock, _MSG)
        frames = _frames_until(sock, _is_error)
    finally:
        sock.close()
    assert frames[-1]["type"] == "error" and frames[-1]["done"] is True
    assert llm_calls == []


def test_global_chat_limit_is_honoured_per_message(
    live_port: int, llm_calls: list, monkeypatch: pytest.MonkeyPatch
) -> None:
    handler = app_module.MediaPlanHandler
    monkeypatch.setattr(handler, "_check_rate_limit", lambda self: True)
    answers = iter([True, False])
    monkeypatch.setattr(
        handler, "_check_global_chat_rate_limit", lambda self: next(answers, False)
    )
    sock = _open(live_port)
    try:
        _send(sock, _MSG)  # allowed: reaches the (stubbed) LLM
        _frames_until(sock, lambda f: f.get("done") is True and "full_response" in f)
        assert llm_calls == ["hello"]
        _send(sock, _MSG)  # global budget spent: refused, LLM not called again
        frames = _frames_until(sock, _is_error)
    finally:
        sock.close()
    assert frames[-1]["code"] == "rate_limited"
    assert llm_calls == ["hello"]


def test_socket_closes_after_consecutive_refusals(
    live_port: int, llm_calls: list, monkeypatch: pytest.MonkeyPatch
) -> None:
    handler = app_module.MediaPlanHandler
    seen = {"n": 0}

    def per_ip(self) -> bool:
        seen["n"] += 1
        return seen["n"] == 1

    monkeypatch.setattr(handler, "_check_rate_limit", per_ip)
    sock = _open(live_port)
    refusals = 0
    try:
        for _ in range(6):
            _send(sock, _MSG)
        with pytest.raises((EOFError, OSError)):
            while True:
                if _is_error(_frame(sock)):
                    refusals += 1
    finally:
        sock.close()
    assert refusals == 5  # the 5th consecutive refusal ends the connection
    assert llm_calls == []
