#!/usr/bin/env python3
"""Smoke tests for Nova AI Suite -- critical endpoint validation."""

import json
import os
import sys
import urllib.request
import urllib.error
import time
from typing import Optional

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_URL = os.environ.get("TEST_BASE_URL", "http://localhost:10000")
TIMEOUT = 15
_results: list[dict] = []


def _fetch(
    path: str, method: str = "GET", data: Optional[dict] = None, timeout: int = TIMEOUT
) -> tuple[int, str]:
    """Fetch a URL and return (status_code, body)."""
    url = f"{BASE_URL}{path}"
    body_bytes = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body_bytes, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode() if e.fp else ""
    except Exception as e:
        return 0, str(e)


def _test(
    name: str,
    path: str,
    method: str = "GET",
    data: Optional[dict] = None,
    expect_status: int = 200,
    expect_contains: Optional[str] = None,
    expect_json_key: Optional[str] = None,
) -> bool:
    """Run a single test and record result."""
    status, body = _fetch(path, method, data)
    passed = status == expect_status
    if passed and expect_contains:
        passed = expect_contains.lower() in body.lower()
    if passed and expect_json_key:
        try:
            passed = expect_json_key in json.loads(body)
        except (json.JSONDecodeError, TypeError):
            passed = False
    _results.append(
        {"name": name, "passed": passed, "status": status, "expected": expect_status}
    )
    return passed


def run_all() -> bool:
    """Run all smoke tests. Returns True if all pass."""
    print(f"\n{'='*60}")
    print(f"  NOVA AI SUITE -- SMOKE TESTS")
    print(f"  Target: {BASE_URL}")
    print(f"{'='*60}\n")

    # ── Page Routes ──
    _test("Homepage (hub)", "/")
    _test("Platform shell", "/platform")
    _test("Media Plan Generator", "/media-plan")
    _test("Budget Simulator", "/simulator")
    _test("Competitive Intel", "/competitive")
    _test("Market Pulse", "/market-pulse")
    _test("Social Planner", "/social-plan")
    _test("Campaign Tracker", "/tracker")
    _test("ComplianceGuard", "/compliance-guard")
    _test("CreativeAI", "/creative-ai")
    _test("API Portal", "/api-portal")

    # ── Health Endpoints ──
    _test("Health check", "/api/health", expect_json_key="status")
    _test("Health ready", "/api/health/ready", expect_json_key="ready")
    _test("Data matrix", "/api/health/data-matrix", expect_json_key="sources")
    _test("Enrichment status", "/api/health/enrichment")
    _test("Integrations", "/api/health/integrations")

    # ── Dashboard Widgets ──
    _test("Dashboard widgets", "/api/dashboard/widgets", expect_json_key="campaigns")

    # ── API Endpoints (GET) ──
    _test("Channels API", "/api/channels", expect_json_key="channels")
    _test("CSRF token", "/api/csrf-token", expect_json_key="csrf_token")

    # ── Chat API ──
    _test(
        "Chat endpoint",
        "/api/chat",
        method="POST",
        data={"message": "hello", "conversation_history": []},
        expect_json_key="response",
    )

    # ── Campaign Save ──
    _test(
        "Campaign save",
        "/api/campaign/save",
        method="POST",
        data={"name": "Smoke Test Campaign", "client": "Test Corp", "budget": 50000},
        expect_json_key="ok",
    )

    # ── Resilience ──
    _test("Resilience status", "/api/resilience/status")

    # ── Static Assets ──
    _test("Nova chat widget JS", "/static/nova-chat.js")
    _test("Robots.txt", "/robots.txt")

    # ── Results ──
    print(f"\n{'─'*60}")
    passed = sum(1 for r in _results if r["passed"])
    failed = sum(1 for r in _results if not r["passed"])
    total = len(_results)

    for r in _results:
        icon = "PASS" if r["passed"] else "FAIL"
        status_info = f"(HTTP {r['status']})" if r["status"] != r["expected"] else ""
        print(f"  [{icon}] {r['name']} {status_info}")

    print(f"\n{'─'*60}")
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    print(f"{'='*60}\n")

    return failed == 0


if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)
