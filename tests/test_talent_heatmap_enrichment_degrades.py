"""POST /api/talent-heatmap/analyze degrades when an optional enrichment
dependency raises an exception type its except clause did not list.

The Supabase salary call caught only (URLError, OSError, ValueError) and the
JobSpy density call only (ValueError, TypeError, KeyError, OSError). Anything
else (a RuntimeError from a client library, an AttributeError from a client
defect) escaped to the handler's outer except and the whole analysis came
back as "Something went wrong" instead of the baseline result. Each optional
enrichment now logs and skips any failure, as the Adzuna and Census siblings
in the same block already did.
"""

from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Iterator

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402


class _LibraryError(Exception):
    """An exception type outside every narrow tuple in the endpoint."""


def _raise_library_error(*_a, **_k):
    raise _LibraryError("simulated third-party client failure")


@pytest.fixture()
def port(monkeypatch) -> Iterator[int]:
    monkeypatch.setattr(app, "_api_integrations_available", False, raising=False)
    monkeypatch.setattr(app, "_generate_product_insights", lambda *a, **k: {}, raising=False)
    import api_enrichment

    monkeypatch.setattr(api_enrichment, "fetch_census_workforce_data", lambda loc: None)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        p = probe.getsockname()[1]
    srv = app.ThreadedHTTPServer(("127.0.0.1", p), app.MediaPlanHandler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", p), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    try:
        yield p
    finally:
        srv.shutdown()
        srv.server_close()


def _post_analyze(p: int) -> tuple:
    body = json.dumps(
        {
            "role": "Software Engineer",
            "industry": "tech_engineering",
            "locations": ["Dallas, TX", "Berlin, Germany"],
            "budget": 100000,
            "num_hires": 2,
        }
    )
    conn = http.client.HTTPConnection("127.0.0.1", p, timeout=60)
    conn.request("GET", "/api/csrf-token")
    page = conn.getresponse()
    page.read()
    cookies = [
        v.split(";", 1)[0] for k, v in page.getheaders() if k.lower() == "set-cookie"
    ]
    token = next(
        (c.split("=", 1)[1] for c in cookies if c.startswith("csrf_token=")), ""
    )
    assert token, f"no csrf_token cookie from the page load: {cookies}"
    conn.request(
        "POST",
        "/api/talent-heatmap/analyze",
        body=body,
        headers={
            "Content-Type": "application/json",
            "Origin": f"http://localhost:{p}",
            "Referer": f"http://localhost:{p}/talent-heatmap",
            "X-CSRF-Token": token,
            "Cookie": "; ".join(cookies),
        },
    )
    resp = conn.getresponse()
    return resp.status, json.loads(resp.read())


def test_supabase_salary_enrichment_failure_degrades(port, monkeypatch):
    monkeypatch.setattr(app, "_supabase_data_available", True, raising=False)
    monkeypatch.setattr(app, "get_salary_data", _raise_library_error, raising=False)
    monkeypatch.setattr(app, "_jobspy_available", False, raising=False)
    status, payload = _post_analyze(port)
    assert status == 200, payload
    assert payload.get("status") != "error", payload
    assert len(payload["difficulties"]) == 2
    assert "supabase_salary_data" not in payload


def test_jobspy_density_enrichment_failure_degrades(port, monkeypatch):
    monkeypatch.setattr(app, "_supabase_data_available", False, raising=False)
    monkeypatch.setattr(app, "_jobspy_available", True, raising=False)
    monkeypatch.setattr(app, "_jobspy_scrape_jobs", _raise_library_error, raising=False)
    status, payload = _post_analyze(port)
    assert status == 200, payload
    assert payload.get("status") != "error", payload
    assert len(payload["difficulties"]) == 2
    assert "jobspy_posting_density" not in (payload.get("live_market_data") or {})
