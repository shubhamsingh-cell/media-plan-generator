"""POST /api/talent-heatmap/analyze survives a live-enrichment client that
lacks a method or raises (2026-11-03).

The endpoint called ``BEAClient.get_regional_employment``, a method that has
never existed (api_integrations.BEAClient). The AttributeError escaped the
enrichment block's narrow except and the whole analysis returned "Something
went wrong". The call is removed and each optional enrichment logs and skips
any failure.
"""

from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import time
import types
from pathlib import Path
from typing import Iterator

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402


class _BEAWithoutRegionalEmployment:
    """A BEA client with none of the methods the endpoint might reach for."""


class _CensusThatRaises:
    def get_population_by_state(self):
        raise AttributeError("simulated client defect")


@pytest.fixture()
def server(monkeypatch) -> Iterator[int]:
    monkeypatch.setattr(app, "_api_integrations_available", True, raising=False)
    monkeypatch.setattr(app, "_api_bea", _BEAWithoutRegionalEmployment(), raising=False)
    monkeypatch.setattr(app, "_api_census", _CensusThatRaises(), raising=False)
    monkeypatch.setattr(app, "_api_adzuna", None, raising=False)
    monkeypatch.setattr(app, "_supabase_data_available", False, raising=False)
    monkeypatch.setattr(app, "_jobspy_available", False, raising=False)
    monkeypatch.setattr(app, "_generate_product_insights", lambda *a, **k: {}, raising=False)
    import api_enrichment

    monkeypatch.setattr(api_enrichment, "fetch_census_workforce_data", lambda loc: None)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    srv = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    try:
        yield port
    finally:
        srv.shutdown()
        srv.server_close()


def test_analyze_returns_a_result_when_a_live_client_lacks_a_method(server):
    body = json.dumps(
        {
            "role": "Software Engineer",
            "industry": "tech_engineering",
            "locations": ["Dallas, TX", "Berlin, Germany"],
            "budget": 100000,
            "num_hires": 2,
        }
    )
    # the page load sets the CSRF cookie the POST must echo (as the browser does)
    conn = http.client.HTTPConnection("127.0.0.1", server, timeout=60)
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
            "Origin": f"http://localhost:{server}",
            "Referer": f"http://localhost:{server}/talent-heatmap",
            "X-CSRF-Token": token,
            "Cookie": "; ".join(cookies),
        },
    )
    resp = conn.getresponse()
    payload = json.loads(resp.read())
    assert resp.status == 200, payload
    assert payload.get("status") != "error", payload
    assert len(payload["difficulties"]) == 2
    assert "bea_regional_employment" not in (payload.get("live_market_data") or {})


def test_the_nonexistent_bea_method_is_no_longer_called():
    source = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")
    assert "_api_bea.get_regional_employment(" not in source
