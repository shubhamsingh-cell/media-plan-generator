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
import os
import socket

import app as app_module
from tests.live_server import live_port  # noqa: F401 (fixture)


def _upgrade(port: int, origin: "str | None") -> int:
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


def test_handshake_gate_mirrors_sse_pair() -> None:
    import inspect

    src = inspect.getsource(app_module.MediaPlanHandler._handle_ws_chat)
    assert "self._check_joveo_auth() or self._check_same_origin_auth()" in src
