"""A loopback stand-in for Supabase PostgREST's ``/rest/v1/cache`` table.

Used by the shared-state tests to exercise the durable layer over a REAL
HTTP boundary (urllib -> socket -> this server) instead of patching the
client. It models the two production facts the durable layer depends on:

* the prod ``cache`` table has ``id`` as its primary key and ``key`` as a
  separate UNIQUE column (constraint ``cache_key_key`` -- the 409s in the
  2026-10-01 telemetry). A ``resolution=merge-duplicates`` upsert WITHOUT
  ``?on_conflict=key`` therefore 409s on the second write of a key; with it,
  the row is updated in place. The fake enforces exactly that.
* reads filter by ``key=eq.<value>`` and return a JSON list of rows.

Not collected by pytest (no ``test_`` prefix). Loopback only, so the
conftest external-network guard never trips.
"""

from __future__ import annotations

import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class FakePostgrest:
    """In-memory ``cache`` table behind a real loopback HTTP server."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.requests: list[tuple[str, str]] = []
        self.fail_with: int = 0  # non-zero -> every request answers this status
        self.lock = threading.Lock()
        fake = self

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: Any) -> None:  # silence test output
                return

            def _reply(self, status: int, body: Any = None) -> None:
                raw = b"" if body is None else json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                if raw:
                    self.wfile.write(raw)

            def _parsed(self) -> tuple[str, dict[str, list[str]]]:
                parsed = urllib.parse.urlparse(self.path)
                return parsed.path, urllib.parse.parse_qs(parsed.query)

            def _gate(self) -> bool:
                with fake.lock:
                    fake.requests.append((self.command, self.path))
                    failing = fake.fail_with
                if failing:
                    self._reply(failing, {"message": "injected failure"})
                    return False
                if not (self.headers.get("apikey") or ""):
                    self._reply(401, {"message": "No API key found in request"})
                    return False
                return True

            def do_GET(self) -> None:  # noqa: N802
                if not self._gate():
                    return
                path, qs = self._parsed()
                if path != "/rest/v1/cache":
                    self._reply(404, {"message": "relation does not exist"})
                    return
                key_filter = (qs.get("key") or [""])[0]
                with fake.lock:
                    if key_filter.startswith("eq."):
                        row = fake.rows.get(key_filter[3:])
                        rows = [dict(row)] if row else []
                    else:
                        rows = [dict(r) for r in fake.rows.values()]
                self._reply(200, rows)

            def do_POST(self) -> None:  # noqa: N802
                if not self._gate():
                    return
                path, qs = self._parsed()
                if path != "/rest/v1/cache":
                    self._reply(404, {"message": "relation does not exist"})
                    return
                length = int(self.headers.get("Content-Length") or 0)
                payload = json.loads(self.rfile.read(length) or b"null")
                rows = payload if isinstance(payload, list) else [payload]
                merge = "merge-duplicates" in (self.headers.get("Prefer") or "")
                on_key = (qs.get("on_conflict") or [""])[0] == "key"
                with fake.lock:
                    for row in rows:
                        k = row.get("key") or ""
                        if k in fake.rows and not (merge and on_key):
                            self._reply(
                                409,
                                {
                                    "code": "23505",
                                    "message": (
                                        "duplicate key value violates unique "
                                        'constraint "cache_key_key"'
                                    ),
                                },
                            )
                            return
                    for row in rows:
                        fake.rows[row.get("key") or ""] = dict(row)
                self._reply(201)

            def do_DELETE(self) -> None:  # noqa: N802
                if not self._gate():
                    return
                _path, qs = self._parsed()
                key_filter = (qs.get("key") or [""])[0]
                with fake.lock:
                    if key_filter.startswith("eq."):
                        fake.rows.pop(key_filter[3:], None)
                self._reply(204)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="fake-postgrest", daemon=True
        )
        self._thread.start()

    def request_count(self, method: str = "") -> int:
        with self.lock:
            return sum(1 for m, _ in self.requests if not method or m == method)

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
