"""Reflected XSS through GET /platform/<section>/<deeper path>.

routes/pages.py copied the request path into an inline script:
``window.__NOVA_INITIAL_ROUTE = "<path>";``. A path carrying a quote or
``</script>`` broke out of the string (the path reaches the router
undecoded, so browsers' %22 stays inert, but raw clients and literal
characters do not). The route must be allow-listed and embedded with the
safe JSON-in-script pattern; these send ten breakout payloads through the
real handler over a raw socket (so the bytes arrive exactly as written).
"""

from __future__ import annotations

import json
import re
import socket
import threading
import time
from pathlib import Path
from typing import Iterator

import pytest

import app
from routes import pages

_PLATFORM_HTML = (Path(__file__).resolve().parent.parent / "templates" / "platform.html").read_text(
    encoding="utf-8"
)
_ROUTE_RE = re.compile(r"<script>window\.__NOVA_INITIAL_ROUTE = (.*?);</script>")

_BREAKOUTS = [
    'x";alert(1);x="',
    "x</script><script>alert(1)</script>",
    'x";</script><script>alert(document.domain)</script>',
    "x<!--<script>alert(1)//",
    "x</SCRIPT/><svg onload=alert(1)>",
    'x"><img src=x onerror=alert(1)>',
    "x\\\";alert(1)//",
    "x'-alert(1)-'",
    "x`;alert(1);//`",
    "x%22;alert(1);x=%22",
]


@pytest.fixture(scope="module")
def server() -> Iterator[int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    srv = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    threading.Thread(target=srv.serve_forever, daemon=True, name="test-route-xss").start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    yield port
    srv.shutdown()
    srv.server_close()


def _raw_get(port: int, target: str) -> tuple[int, str]:
    """GET with the request target sent byte-for-byte (no client escaping),
    except spaces, which cannot appear in a request line at all."""
    target = target.replace(" ", "%20")
    with socket.create_connection(("127.0.0.1", port), timeout=20) as sock:
        sock.sendall(
            f"GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/html\r\n"
            "Connection: close\r\n\r\n".encode("latin-1")
        )
        chunks = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    raw = b"".join(chunks).decode("utf-8", "replace")
    head, _, body = raw.partition("\r\n\r\n")
    return int(head.split(" ", 2)[1]), body


@pytest.mark.parametrize("payload", _BREAKOUTS, ids=[f"p{i}" for i in range(len(_BREAKOUTS))])
def test_platform_route_breakouts_are_neutralised(server: int, payload: str) -> None:
    status, page = _raw_get(server, f"/platform/plan/{payload}/y")
    assert status == 200, page[:200]
    injected = _ROUTE_RE.findall(page)
    assert len(injected) == 1, "the route script must appear exactly once, intact"
    route = json.loads(injected[0])  # a valid JSON string literal, nothing after it
    assert route == pages._PLATFORM_SUB_ROUTES["plan"]["initial_route"], route
    assert page.lower().count("<script") == _PLATFORM_HTML.lower().count("<script") + 1
    for marker in ("alert(1)", "alert(document.domain)", "onerror=", "onload=", "<svg"):
        # The template has its own onload= (stylesheet preload): compare counts.
        assert page.count(marker) == _PLATFORM_HTML.count(marker), f"{marker!r} reflected"


def test_legitimate_deep_routes_still_reach_the_router(server: int) -> None:
    for route in ("plan/budget", "intelligence/talent/hire-signal", "plan/ab_test-2"):
        status, page = _raw_get(server, f"/platform/{route}")
        assert status == 200
        assert json.loads(_ROUTE_RE.findall(page)[0]) == route


def test_top_level_section_uses_its_default_route(server: int) -> None:
    status, page = _raw_get(server, "/platform/plan")
    assert status == 200
    assert json.loads(_ROUTE_RE.findall(page)[0]) == "plan/campaign"


@pytest.mark.parametrize(
    "value",
    ["</script><script>alert(1)</script>", "<!--", "a b c", "x&y>z", '"; alert(1); "'],
)
def test_inline_script_json_never_contains_html_breakout_sequences(value: str) -> None:
    encoded = pages._json_for_inline_script(value)
    assert json.loads(encoded) == value
    for bad in ("<", ">", "&", " ", " "):
        assert bad not in encoded
