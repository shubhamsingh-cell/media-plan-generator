"""One real app server process for the multi-worker shared-state tests.

Production runs ``gunicorn --workers 4 --preload`` on ONE Render instance:
several processes, one filesystem, no shared memory. Each process started
from this script is one such "worker": it imports the real ``app`` module,
serves the real ``MediaPlanHandler`` on its own loopback port, and shares the
filesystem state directory (``NOVA_SLOT_DIR``) with its siblings. Separate
ports make "written on worker A, read on worker B" deterministic, which
gunicorn's kernel load-balancing never is.

Protocol: prints ``READY <port>`` once serving. Then reads one JSON command
per stdin line and answers one JSON line on stdout. Commands drive the SAME
code paths the running server uses to write state (``_store_plan_result``,
the async-job dict update + ``_mirror_job``) for state that has no cheap
HTTP trigger (a real /api/generate takes 30+ s and calls external APIs).

Every non-loopback connect is refused before ``app`` is imported, mirroring
tests/conftest.py's network guard -- these processes never inherit it.

Not collected by pytest (no ``test_`` prefix).
"""

from __future__ import annotations

import base64
import json
import os
import socket
import sys
import threading
import time
from typing import Any

_REAL_CONNECT = socket.socket.connect


def _guarded_connect(self: socket.socket, address: Any) -> Any:
    host = ""
    if isinstance(address, tuple) and address:
        host = str(address[0]).strip("[]").lower()
    if isinstance(address, (str, bytes)) or host in ("localhost", "::1") or host.startswith(
        "127."
    ):
        return _REAL_CONNECT(self, address)
    raise ConnectionRefusedError(f"shared_state_worker: external connect blocked {address!r}")


socket.socket.connect = _guarded_connect  # type: ignore[method-assign]
os.environ["NOVA_DISABLE_AUTO_QC"] = "1"

# stdout is the command channel; anything the app prints goes to stderr.
_PROTO = sys.stdout
sys.stdout = sys.stderr

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402


def _make_job(cmd: dict) -> dict:
    """Replay the async-generate write sequence: create -> mirror ->
    complete/fail -> mirror (app.py's POST /api/generate X-Async path)."""
    job_id = cmd["job_id"]
    with app._generation_jobs_lock:
        app._generation_jobs[job_id] = {
            "status": "processing",
            "progress_pct": 0,
            "status_message": "Starting generation...",
            "created": time.time() - float(cmd.get("age") or 0),
            "result_bytes": None,
            "result_content_type": None,
            "result_filename": None,
            "error": None,
            "_session_token": cmd.get("session") or "",
        }
    app._mirror_job(job_id)
    status = cmd.get("status") or "processing"
    if status == "processing":
        with app._generation_jobs_lock:
            app._generation_jobs[job_id]["progress_pct"] = 40
            app._generation_jobs[job_id]["status_message"] = "Synthesizing..."
        app._mirror_job(job_id)
    elif status == "completed":
        with app._generation_jobs_lock:
            app._generation_jobs[job_id].update(
                {
                    "status": "completed",
                    "progress_pct": 100,
                    "status_message": "Complete",
                    "bundle_qa": cmd.get("bundle_qa"),
                    "result_bytes": base64.b64decode(cmd.get("result_b64") or ""),
                    "result_content_type": "application/zip",
                    "result_filename": cmd.get("filename") or "plan.zip",
                }
            )
        app._mirror_job(job_id)
    elif status == "failed":
        with app._generation_jobs_lock:
            app._generation_jobs[job_id].update(
                {"status": "failed", "error": "boom", "result_bytes": None}
            )
        app._mirror_job(job_id)
    return {"ok": True}


def _handle(cmd: dict) -> dict:
    op = cmd.get("op") or ""
    if op == "ping":
        return {"ok": True, "pid": os.getpid()}
    if op == "make_job":
        return _make_job(cmd)
    if op == "store_plan_result":
        app._store_plan_result(cmd["plan_id"], cmd.get("data") or {})
        return {"ok": True}
    if op == "set_sheets_url":  # what the async Sheets export does on success
        app._plan_result_set_sheets_url(cmd["plan_id"], cmd["url"])
        return {"ok": True}
    if op == "flush":
        flush = getattr(app, "_shared_state_flush", None)
        return {"ok": True, "flushed": bool(flush(float(cmd.get("timeout") or 5))) if flush else False}
    if op == "job_record":
        getter = getattr(app, "_job_record_get", None)
        return {"ok": True, "record": getter(cmd["job_id"]) if getter else None}
    if op == "memory_has":
        with app._shared_plans_lock:
            in_shares = cmd["key"] in app._shared_plans
        with app._plan_results_lock:
            in_results = cmd["key"] in app._plan_results_store
        with app._generation_jobs_lock:
            in_jobs = cmd["key"] in app._generation_jobs
        return {"ok": True, "shares": in_shares, "results": in_results, "jobs": in_jobs}
    return {"ok": False, "error": f"unknown op {op!r}"}


def main() -> None:
    server = app.ThreadedHTTPServer(("127.0.0.1", 0), app.MediaPlanHandler)
    threading.Thread(target=server.serve_forever, daemon=True, name="worker-http").start()
    _PROTO.write(f"READY {server.server_address[1]}\n")
    _PROTO.flush()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            reply = _handle(json.loads(line))
        except Exception as exc:  # report, never die mid-protocol
            reply = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        _PROTO.write(json.dumps(reply, default=str) + "\n")
        _PROTO.flush()
    server.shutdown()


if __name__ == "__main__":
    main()
