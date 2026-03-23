#!/usr/bin/env python3
"""Comprehensive Stress Test for Nova AI Suite.

Tests all backend APIs, frontend templates, integrations, data quality,
performance, and security against the live deployment on Render.com.
"""

import json
import time
import urllib.request
import urllib.error
import urllib.parse
import ssl
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
from datetime import datetime, timedelta

# ── Configuration ──
BASE_URL = "https://media-plan-generator.onrender.com"
LOCAL_PROJECT = Path(__file__).parent.parent
TEMPLATES_DIR = LOCAL_PROJECT / "templates"
DATA_DIR = LOCAL_PROJECT / "data"

# SSL context (skip verification for testing)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# ── Results Storage ──
findings: List[Dict[str, Any]] = []
test_count = 0
pass_count = 0
fail_count = 0


def record(
    severity: str,
    category: str,
    test_name: str,
    description: str,
    response_time: float = 0.0,
    status_code: int = 0,
) -> None:
    """Record a test finding."""
    findings.append(
        {
            "severity": severity,
            "category": category,
            "test": test_name,
            "description": description,
            "response_time_ms": round(response_time * 1000, 1),
            "status_code": status_code,
        }
    )


def http_get(path: str, timeout: int = 30) -> Tuple[int, Any, float]:
    """Make a GET request. Returns (status_code, body, response_time_seconds)."""
    global test_count, pass_count, fail_count
    test_count += 1
    url = f"{BASE_URL}{path}"
    start = time.time()
    try:
        req = urllib.request.Request(url, method="GET")
        req.add_header("Accept", "application/json, text/html")
        req.add_header("User-Agent", "NovaStressTest/1.0")
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            elapsed = time.time() - start
            body = resp.read()
            content_type = resp.headers.get("Content-Type", "")
            status = resp.status
            if "application/json" in content_type:
                try:
                    body = json.loads(body)
                except json.JSONDecodeError:
                    body = body.decode("utf-8", errors="replace")
            elif "text/html" in content_type:
                body = body.decode("utf-8", errors="replace")
            pass_count += 1
            return status, body, elapsed
    except urllib.error.HTTPError as e:
        elapsed = time.time() - start
        fail_count += 1
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = str(e)
        return e.code, err_body, elapsed
    except Exception as e:
        elapsed = time.time() - start
        fail_count += 1
        return 0, str(e), elapsed


def http_post(
    path: str,
    data: Any = None,
    timeout: int = 30,
    csrf_token: str = "",
    csrf_cookie: str = "",
) -> Tuple[int, Any, float]:
    """Make a POST request with JSON body. Returns (status_code, body, response_time_seconds)."""
    global test_count, pass_count, fail_count
    test_count += 1
    url = f"{BASE_URL}{path}"
    body_bytes = json.dumps(data or {}).encode("utf-8")
    start = time.time()
    try:
        req = urllib.request.Request(url, data=body_bytes, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", "NovaStressTest/1.0")
        if csrf_token:
            req.add_header("X-CSRF-Token", csrf_token)
        if csrf_cookie:
            req.add_header("Cookie", csrf_cookie)
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            elapsed = time.time() - start
            resp_body = resp.read()
            content_type = resp.headers.get("Content-Type", "")
            status = resp.status
            if "application/json" in content_type:
                try:
                    resp_body = json.loads(resp_body)
                except json.JSONDecodeError:
                    resp_body = resp_body.decode("utf-8", errors="replace")
            else:
                resp_body = resp_body.decode("utf-8", errors="replace")
            pass_count += 1
            return status, resp_body, elapsed
    except urllib.error.HTTPError as e:
        elapsed = time.time() - start
        fail_count += 1
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = str(e)
        try:
            err_body = json.loads(err_body)
        except (json.JSONDecodeError, TypeError):
            pass
        return e.code, err_body, elapsed
    except Exception as e:
        elapsed = time.time() - start
        fail_count += 1
        return 0, str(e), elapsed


def get_csrf_token() -> Tuple[str, str]:
    """Get CSRF token and cookie from the server."""
    url = f"{BASE_URL}/api/csrf-token"
    try:
        req = urllib.request.Request(url, method="GET")
        req.add_header("User-Agent", "NovaStressTest/1.0")
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            body = json.loads(resp.read())
            token = body.get("token", "")
            # Extract Set-Cookie header
            cookie_header = resp.headers.get("Set-Cookie", "")
            # Build cookie string for subsequent requests
            cookie_val = ""
            if cookie_header:
                # Extract csrf_token=xxx part
                for part in cookie_header.split(";"):
                    part = part.strip()
                    if part.startswith("csrf_token="):
                        cookie_val = part
                        break
            return token, cookie_val
    except Exception as e:
        print(f"  [WARN] Failed to get CSRF token: {e}")
        return "", ""


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 1: BACKEND API HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
def test_backend_apis() -> None:
    """Test all backend API endpoints."""
    print("\n" + "=" * 70)
    print("TEST SUITE 1: BACKEND API HEALTH")
    print("=" * 70)

    # ── GET endpoints ──
    get_endpoints = [
        ("/health", "Health Check"),
        ("/api/health", "Detailed Health Check"),
        ("/api/csrf-token", "CSRF Token"),
        ("/api/health/ready", "Readiness Probe"),
        ("/api/firecrawl/status", "Firecrawl Status"),
        ("/api/market-pulse/news", "Market Pulse News"),
        ("/api/channels", "Channels List"),
        ("/api/docs/openapi.json", "OpenAPI Spec"),
        ("/robots.txt", "Robots.txt"),
        ("/sitemap.xml", "Sitemap"),
        ("/docs", "Swagger UI"),
    ]

    for path, name in get_endpoints:
        status, body, elapsed = http_get(path)
        print(f"  GET {path:45s} -> {status} ({elapsed*1000:.0f}ms)")
        if status == 0:
            record(
                "CRITICAL",
                "API",
                f"GET {path}",
                f"{name} connection failed: {body}",
                elapsed,
                status,
            )
        elif status >= 500:
            record(
                "CRITICAL",
                "API",
                f"GET {path}",
                f"{name} returned {status}",
                elapsed,
                status,
            )
        elif status >= 400 and status != 401:
            record(
                "HIGH",
                "API",
                f"GET {path}",
                f"{name} returned {status}",
                elapsed,
                status,
            )
        elif elapsed > 10:
            record(
                "CRITICAL",
                "Performance",
                f"GET {path}",
                f"{name} took {elapsed:.1f}s (>10s)",
                elapsed,
                status,
            )
        elif elapsed > 5:
            record(
                "HIGH",
                "Performance",
                f"GET {path}",
                f"{name} took {elapsed:.1f}s (>5s)",
                elapsed,
                status,
            )
        elif elapsed > 3:
            record(
                "MEDIUM",
                "Performance",
                f"GET {path}",
                f"{name} took {elapsed:.1f}s (>3s)",
                elapsed,
                status,
            )
        else:
            record("PASS", "API", f"GET {path}", f"{name} OK", elapsed, status)

        # Validate health check structure
        if path in ("/health", "/api/health") and isinstance(body, dict):
            if "status" not in body:
                record(
                    "HIGH",
                    "API",
                    f"GET {path}",
                    "Health check missing 'status' field",
                    elapsed,
                    status,
                )
            if body.get("status") != "healthy":
                record(
                    "HIGH",
                    "API",
                    f"GET {path}",
                    f"Health status is '{body.get('status')}' (not 'healthy')",
                    elapsed,
                    status,
                )

    # ── POST endpoints (require CSRF token) ──
    print("\n  Acquiring CSRF token...")
    csrf_token, csrf_cookie = get_csrf_token()
    if not csrf_token:
        record(
            "CRITICAL",
            "Auth",
            "CSRF Token",
            "Failed to acquire CSRF token - all POST tests will fail",
        )
        print("  [CRITICAL] No CSRF token obtained!")
        return

    print(f"  CSRF token acquired: {csrf_token[:8]}...")

    post_endpoints = [
        (
            "/api/chat",
            {
                "message": "What are the best job boards for software engineers?",
                "history": [],
            },
            "Nova Chat",
        ),
        (
            "/api/simulator/simulate",
            {
                "channel_allocations": {
                    "Indeed": 40,
                    "LinkedIn": 30,
                    "Google Ads": 20,
                    "Glassdoor": 10,
                },
                "total_budget": 100000,
                "industry": "Technology",
                "roles": ["Software Engineer"],
                "locations": ["United States"],
            },
            "Budget Simulator",
        ),
        (
            "/api/talent-heatmap/analyze",
            {
                "role": "Software Engineer",
                "industry": "technology",
                "locations": ["San Francisco", "New York", "Austin"],
                "budget": 50000,
                "num_hires": 5,
            },
            "Talent Heatmap",
        ),
        (
            "/api/compliance/analyze",
            {
                "location": "California",
                "industry": "Technology",
                "role": "Software Engineer",
            },
            "Compliance Analyze",
        ),
        (
            "/api/creative/generate",
            {
                "role": "Software Engineer",
                "industry": "Technology",
                "company": "TestCorp",
                "tone": "professional",
            },
            "Creative Generate",
        ),
        (
            "/api/post-campaign/summary",
            {
                "campaign_name": "Q1 Engineering Hiring",
                "total_spend": 50000,
                "total_applications": 500,
                "total_hires": 10,
                "channels": {
                    "Indeed": {"spend": 20000, "applications": 200},
                    "LinkedIn": {"spend": 30000, "applications": 300},
                },
            },
            "Post-Campaign Summary",
        ),
        (
            "/api/roi/calculate",
            {
                "total_budget": 100000,
                "total_hires": 15,
                "avg_salary": 120000,
                "time_to_fill_days": 45,
                "industry": "Technology",
            },
            "ROI Calculator",
        ),
        (
            "/api/ab-test/generate",
            {
                "role": "Software Engineer",
                "industry": "Technology",
                "channels": ["Indeed", "LinkedIn"],
            },
            "A/B Test Generate",
        ),
        (
            "/api/vendor-iq/live-pricing",
            {
                "vendors": ["indeed", "linkedin", "glassdoor"],
                "industry": "Technology",
            },
            "Vendor IQ Pricing",
        ),
        (
            "/api/payscale-sync/salary",
            {
                "role": "Software Engineer",
                "location": "San Francisco",
            },
            "PayScale Salary",
        ),
        (
            "/api/social-plan/generate",
            {
                "role": "Software Engineer",
                "industry": "Technology",
                "company": "TestCorp",
                "locations": ["San Francisco"],
                "budget": 10000,
            },
            "Social Plan Generate",
        ),
        (
            "/api/competitive/scrape",
            {
                "competitors": ["google.com", "meta.com"],
                "role": "Software Engineer",
            },
            "Competitive Scrape",
        ),
        (
            "/api/hire-signal/analyze",
            {
                "manual_data": {
                    "role": "Software Engineer",
                    "location": "San Francisco",
                    "applications": 150,
                    "interviews": 30,
                    "offers": 10,
                    "hires": 5,
                    "days_open": 60,
                    "budget": 25000,
                },
                "industry": "technology",
                "client_name": "TestCorp",
            },
            "HireSignal Analyze",
        ),
    ]

    for path, payload, name in post_endpoints:
        status, body, elapsed = http_post(
            path, payload, timeout=60, csrf_token=csrf_token, csrf_cookie=csrf_cookie
        )
        print(f"  POST {path:44s} -> {status} ({elapsed*1000:.0f}ms)")
        if status == 0:
            record(
                "CRITICAL",
                "API",
                f"POST {path}",
                f"{name} connection failed: {body}",
                elapsed,
                status,
            )
        elif status == 403:
            # Might be CSRF issue
            record(
                "HIGH",
                "API",
                f"POST {path}",
                f"{name} CSRF rejected (403)",
                elapsed,
                status,
            )
        elif status >= 500:
            record(
                "CRITICAL",
                "API",
                f"POST {path}",
                f"{name} returned {status}: {str(body)[:200]}",
                elapsed,
                status,
            )
        elif status >= 400:
            record(
                "HIGH",
                "API",
                f"POST {path}",
                f"{name} returned {status}: {str(body)[:200]}",
                elapsed,
                status,
            )
        elif elapsed > 10:
            record(
                "CRITICAL",
                "Performance",
                f"POST {path}",
                f"{name} took {elapsed:.1f}s (>10s)",
                elapsed,
                status,
            )
        elif elapsed > 5:
            record(
                "HIGH",
                "Performance",
                f"POST {path}",
                f"{name} took {elapsed:.1f}s (>5s)",
                elapsed,
                status,
            )
        elif elapsed > 3:
            record(
                "MEDIUM",
                "Performance",
                f"POST {path}",
                f"{name} took {elapsed:.1f}s (>3s)",
                elapsed,
                status,
            )
        else:
            record("PASS", "API", f"POST {path}", f"{name} OK", elapsed, status)

        # Validate response structure
        if isinstance(body, dict):
            if "error" in body and status < 400:
                record(
                    "MEDIUM",
                    "API",
                    f"POST {path}",
                    f"{name} returned 200 but has error field: {str(body.get('error'))[:100]}",
                    elapsed,
                    status,
                )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 2: FRONTEND TEMPLATE TESTS
# ═══════════════════════════════════════════════════════════════════════════════
def test_frontend_templates() -> None:
    """Test all frontend template routes."""
    print("\n" + "=" * 70)
    print("TEST SUITE 2: FRONTEND TEMPLATE TESTS")
    print("=" * 70)

    template_routes = [
        ("/", "Hub (root)"),
        ("/hub", "Hub"),
        ("/media-plan", "Media Plan Generator"),
        ("/nova", "Nova Chatbot"),
        ("/vendor-iq", "VendorIQ"),
        ("/hire-signal", "HireSignal"),
        ("/talent-heatmap", "Talent Heatmap"),
        ("/competitive-intel", "Competitive Intel"),
        ("/compliance-guard", "Compliance Guard"),
        ("/creative-ai", "Creative AI"),
        ("/payscale-sync", "PayScale Sync"),
        ("/social-plan", "Social Plan"),
        ("/market-pulse", "Market Pulse"),
        ("/budget-simulator", "Budget Simulator"),
        ("/budget-engine", "Budget Engine"),
        ("/auto-qc", "Auto QC"),
        ("/performance-tracker", "Performance Tracker"),
        ("/market-intel-reports", "Market Intel Reports"),
        ("/eval-framework", "Eval Framework"),
        ("/roi-calculator", "ROI Calculator"),
        ("/skill-target", "Skill Target"),
        ("/applyflow", "ApplyFlow Demo"),
        ("/ab-testing", "A/B Testing"),
        ("/api-portal", "API Portal"),
        ("/pricing", "Pricing"),
        ("/privacy", "Privacy"),
        ("/terms", "Terms"),
        ("/quick-plan", "Quick Plan"),
        ("/quick-brief", "Quick Brief"),
        ("/post-campaign", "Post Campaign"),
        ("/docs", "API Docs"),
    ]

    for path, name in template_routes:
        status, body, elapsed = http_get(path)
        is_html = isinstance(body, str) and (
            "<html" in body.lower() or "<!doctype" in body.lower()
        )
        print(
            f"  GET {path:30s} -> {status} ({elapsed*1000:.0f}ms) {'HTML' if is_html else 'OTHER'}"
        )

        if status == 0:
            record(
                "CRITICAL",
                "Frontend",
                f"GET {path}",
                f"{name} connection failed",
                elapsed,
                status,
            )
            continue
        elif status >= 500:
            record(
                "CRITICAL",
                "Frontend",
                f"GET {path}",
                f"{name} returned {status}",
                elapsed,
                status,
            )
            continue
        elif status == 404:
            record(
                "HIGH",
                "Frontend",
                f"GET {path}",
                f"{name} returned 404 (page not found)",
                elapsed,
                status,
            )
            continue
        elif status == 401:
            record(
                "LOW",
                "Frontend",
                f"GET {path}",
                f"{name} requires auth (401)",
                elapsed,
                status,
            )
            continue
        elif status != 200:
            record(
                "MEDIUM",
                "Frontend",
                f"GET {path}",
                f"{name} returned unexpected {status}",
                elapsed,
                status,
            )
            continue

        if not is_html:
            record(
                "MEDIUM",
                "Frontend",
                f"GET {path}",
                f"{name} did not return HTML",
                elapsed,
                status,
            )
            continue

        # Check for PostHog analytics
        if "posthog" not in body.lower() and "us.i.posthog.com" not in body:
            record(
                "LOW",
                "Frontend",
                f"GET {path}",
                f"{name} missing PostHog analytics snippet",
                elapsed,
                status,
            )

        # Check for brand colors
        has_brand = any(
            c in body
            for c in ["#202058", "#5A54BD", "#6BB3CD", "202058", "5A54BD", "6BB3CD"]
        )
        if not has_brand:
            record(
                "LOW",
                "Frontend",
                f"GET {path}",
                f"{name} missing brand colors",
                elapsed,
                status,
            )

        # Check for LinkedIn footer
        if "chandel13" not in body.lower() and path not in ("/docs",):
            record(
                "LOW",
                "Frontend",
                f"GET {path}",
                f"{name} missing LinkedIn footer link (chandel13)",
                elapsed,
                status,
            )

        # Performance
        if elapsed > 5:
            record(
                "HIGH",
                "Performance",
                f"GET {path}",
                f"{name} template took {elapsed:.1f}s (>5s)",
                elapsed,
                status,
            )
        elif elapsed > 3:
            record(
                "MEDIUM",
                "Performance",
                f"GET {path}",
                f"{name} template took {elapsed:.1f}s (>3s)",
                elapsed,
                status,
            )
        else:
            record("PASS", "Frontend", f"GET {path}", f"{name} OK", elapsed, status)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 3: MEDIA PLAN GENERATOR DEEP TEST
# ═══════════════════════════════════════════════════════════════════════════════
def test_media_plan_generator() -> None:
    """Deep test the flagship media plan generator."""
    print("\n" + "=" * 70)
    print("TEST SUITE 3: MEDIA PLAN GENERATOR DEEP TEST")
    print("=" * 70)

    csrf_token, csrf_cookie = get_csrf_token()
    if not csrf_token:
        record(
            "CRITICAL", "Generator", "CSRF", "Cannot test generator without CSRF token"
        )
        return

    payload = {
        "client_name": "StressTest Corp",
        "requester_name": "QA Bot",
        "requester_email": "qa@test.com",
        "target_roles": [
            {"title": "Software Engineer", "seniority": "Mid-Level", "openings": 5}
        ],
        "locations": ["San Francisco, CA", "New York, NY"],
        "budget": "50000",
        "industry": "Technology",
        "timeline": "90 days",
        "output_format": "json",
    }

    print("  Sending full media plan generation request...")
    status, body, elapsed = http_post(
        "/api/generate",
        payload,
        timeout=120,
        csrf_token=csrf_token,
        csrf_cookie=csrf_cookie,
    )
    print(f"  POST /api/generate -> {status} ({elapsed*1000:.0f}ms)")

    if status == 0:
        record(
            "CRITICAL",
            "Generator",
            "POST /api/generate",
            f"Connection failed: {body}",
            elapsed,
            status,
        )
        return
    elif status == 403:
        record(
            "HIGH",
            "Generator",
            "POST /api/generate",
            "CSRF rejected - check double-submit cookie flow",
            elapsed,
            status,
        )
        return
    elif status >= 500:
        record(
            "CRITICAL",
            "Generator",
            "POST /api/generate",
            f"Server error {status}: {str(body)[:300]}",
            elapsed,
            status,
        )
        return
    elif status >= 400:
        record(
            "HIGH",
            "Generator",
            "POST /api/generate",
            f"Client error {status}: {str(body)[:300]}",
            elapsed,
            status,
        )
        return

    # The generate endpoint returns a ZIP file (binary), not JSON
    if isinstance(body, str) and ("PK" in body[:10] or len(body) > 1000):
        record(
            "PASS",
            "Generator",
            "POST /api/generate",
            f"Media plan ZIP generated successfully ({len(body)} bytes)",
            elapsed,
            status,
        )
    elif isinstance(body, dict):
        # JSON response - check structure
        if "error" in body:
            record(
                "HIGH",
                "Generator",
                "POST /api/generate",
                f"Generator returned error: {body.get('error')}",
                elapsed,
                status,
            )
        else:
            record(
                "PASS",
                "Generator",
                "POST /api/generate",
                "Generator returned JSON response",
                elapsed,
                status,
            )
    else:
        record(
            "PASS",
            "Generator",
            "POST /api/generate",
            f"Generator returned response ({elapsed:.1f}s)",
            elapsed,
            status,
        )

    if elapsed > 30:
        record(
            "HIGH",
            "Performance",
            "POST /api/generate",
            f"Generation took {elapsed:.1f}s (>30s)",
            elapsed,
            status,
        )
    elif elapsed > 15:
        record(
            "MEDIUM",
            "Performance",
            "POST /api/generate",
            f"Generation took {elapsed:.1f}s (>15s)",
            elapsed,
            status,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 4: NOVA CHATBOT DEEP TEST
# ═══════════════════════════════════════════════════════════════════════════════
def test_nova_chatbot() -> None:
    """Deep test the Nova chatbot."""
    print("\n" + "=" * 70)
    print("TEST SUITE 4: NOVA CHATBOT DEEP TEST")
    print("=" * 70)

    csrf_token, csrf_cookie = get_csrf_token()
    if not csrf_token:
        record("CRITICAL", "Chatbot", "CSRF", "Cannot test chatbot without CSRF token")
        return

    test_messages = [
        ("What's the best strategy for hiring nurses in Texas?", "domain-specific"),
        ("Compare Indeed vs LinkedIn for tech recruitment", "vendor-comparison"),
        ("What is the average CPC for healthcare job ads?", "benchmark-query"),
    ]

    for msg, test_type in test_messages:
        payload = {"message": msg, "history": []}
        status, body, elapsed = http_post(
            "/api/chat",
            payload,
            timeout=60,
            csrf_token=csrf_token,
            csrf_cookie=csrf_cookie,
        )
        print(f"  Chat [{test_type}] -> {status} ({elapsed*1000:.0f}ms)")

        if status == 0:
            record(
                "CRITICAL",
                "Chatbot",
                f"Chat ({test_type})",
                f"Connection failed: {body}",
                elapsed,
                status,
            )
            continue
        if status == 403:
            record(
                "HIGH",
                "Chatbot",
                f"Chat ({test_type})",
                "CSRF rejected",
                elapsed,
                status,
            )
            continue
        if status >= 500:
            record(
                "CRITICAL",
                "Chatbot",
                f"Chat ({test_type})",
                f"Server error {status}",
                elapsed,
                status,
            )
            continue

        if isinstance(body, dict):
            response_text = body.get("response", "")
            confidence = body.get("confidence", 0)
            sources = body.get("sources", [])
            tools = body.get("tools_used", [])

            if not response_text:
                record(
                    "HIGH",
                    "Chatbot",
                    f"Chat ({test_type})",
                    "Empty response text",
                    elapsed,
                    status,
                )
            elif len(response_text) < 50:
                record(
                    "MEDIUM",
                    "Chatbot",
                    f"Chat ({test_type})",
                    f"Very short response ({len(response_text)} chars)",
                    elapsed,
                    status,
                )
            else:
                record(
                    "PASS",
                    "Chatbot",
                    f"Chat ({test_type})",
                    f"Response OK ({len(response_text)} chars, confidence={confidence:.2f})",
                    elapsed,
                    status,
                )

            if "error" in body:
                record(
                    "HIGH",
                    "Chatbot",
                    f"Chat ({test_type})",
                    f"Error in response: {body.get('error')}",
                    elapsed,
                    status,
                )
        else:
            record(
                "MEDIUM",
                "Chatbot",
                f"Chat ({test_type})",
                f"Unexpected response type: {type(body).__name__}",
                elapsed,
                status,
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 5: INTEGRATION HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
def test_integration_health() -> None:
    """Test Supabase, PostHog, Firecrawl, enrichment integrations."""
    print("\n" + "=" * 70)
    print("TEST SUITE 5: INTEGRATION HEALTH")
    print("=" * 70)

    # Health endpoint contains integration status
    status, body, elapsed = http_get("/api/health")
    print(f"  /api/health -> {status} ({elapsed*1000:.0f}ms)")

    if isinstance(body, dict):
        # Check Supabase
        sb_status = body.get("supabase")
        if sb_status:
            print(f"    Supabase: {sb_status}")
            if sb_status in ("connected", "ok", True):
                record(
                    "PASS",
                    "Integration",
                    "Supabase",
                    f"Supabase connected: {sb_status}",
                )
            else:
                record(
                    "MEDIUM", "Integration", "Supabase", f"Supabase status: {sb_status}"
                )
        else:
            record(
                "MEDIUM",
                "Integration",
                "Supabase",
                "Supabase status not in health response",
            )

        # Check modules loaded
        modules = body.get("modules") or body.get("loaded_modules") or {}
        if modules:
            print(f"    Modules: {modules}")
        else:
            print("    Modules: (not in health response)")

        # Check enrichment
        enrichment = body.get("enrichment") or body.get("enrichment_daemon")
        if enrichment:
            print(f"    Enrichment: {enrichment}")
        else:
            print("    Enrichment: (not in health response)")

        # Check knowledge base
        kb_loaded = body.get("knowledge_base_loaded") or body.get("kb_loaded")
        if kb_loaded:
            record("PASS", "Integration", "Knowledge Base", f"KB loaded: {kb_loaded}")
        else:
            record(
                "MEDIUM",
                "Integration",
                "Knowledge Base",
                "KB status not in health response",
            )

        # Check for exposed keys in health
        body_str = json.dumps(body)
        for key_pattern in ["phx_", "sk-", "rnd_", "ghp_", "sntrys_"]:
            if key_pattern in body_str:
                record(
                    "CRITICAL",
                    "Security",
                    "Health Endpoint Key Exposure",
                    f"Found '{key_pattern}*' pattern in /api/health response",
                )
    else:
        record(
            "HIGH",
            "Integration",
            "Health Check",
            f"Health check returned non-JSON: {type(body).__name__}",
        )

    # Firecrawl status
    status, body, elapsed = http_get("/api/firecrawl/status")
    print(f"  /api/firecrawl/status -> {status} ({elapsed*1000:.0f}ms)")
    if isinstance(body, dict):
        fc_available = body.get("available") or body.get("status")
        record(
            "PASS" if fc_available else "LOW",
            "Integration",
            "Firecrawl",
            f"Firecrawl status: {fc_available}",
        )

    # CORS check
    print("  Testing CORS headers...")
    try:
        url = f"{BASE_URL}/api/health"
        req = urllib.request.Request(url, method="OPTIONS")
        req.add_header("Origin", "https://media-plan-generator.onrender.com")
        req.add_header("Access-Control-Request-Method", "POST")
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            cors_origin = resp.headers.get("Access-Control-Allow-Origin", "")
            cors_methods = resp.headers.get("Access-Control-Allow-Methods", "")
            print(f"    CORS Origin: {cors_origin}")
            print(f"    CORS Methods: {cors_methods}")
            if cors_origin:
                record("PASS", "Security", "CORS", f"CORS configured: {cors_origin}")
            else:
                record("MEDIUM", "Security", "CORS", "No CORS headers on OPTIONS")
    except Exception as e:
        record("LOW", "Security", "CORS", f"OPTIONS request failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 6: DATA QUALITY CHECK
# ═══════════════════════════════════════════════════════════════════════════════
def test_data_quality() -> None:
    """Check local data files for freshness and integrity."""
    print("\n" + "=" * 70)
    print("TEST SUITE 6: DATA QUALITY CHECK")
    print("=" * 70)

    critical_files = [
        "recruitment_industry_knowledge.json",
        "channels_db.json",
        "joveo_publishers.json",
        "global_supply.json",
        "recruitment_benchmarks_deep.json",
        "platform_intelligence_deep.json",
        "regional_hiring_intelligence.json",
        "enrichment_state.json",
        "live_market_data.json",
    ]

    now = datetime.now()
    stale_threshold = timedelta(days=7)
    very_stale_threshold = timedelta(days=30)

    for filename in critical_files:
        fpath = DATA_DIR / filename
        if not fpath.exists():
            record("HIGH", "Data", filename, f"File does not exist: {fpath}")
            print(f"  {filename:50s} MISSING")
            continue

        stat = fpath.stat()
        size = stat.st_size
        mtime = datetime.fromtimestamp(stat.st_mtime)
        age = now - mtime

        print(
            f"  {filename:50s} {size:>10,} bytes  modified {mtime.strftime('%Y-%m-%d %H:%M')}"
        )

        if size == 0:
            record("CRITICAL", "Data", filename, "File is empty (0 bytes)")
            continue

        if size < 100:
            record("HIGH", "Data", filename, f"File suspiciously small ({size} bytes)")

        # Check JSON validity
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and len(data) == 0:
                record("HIGH", "Data", filename, "JSON file is an empty object {}")
            elif isinstance(data, list) and len(data) == 0:
                record("HIGH", "Data", filename, "JSON file is an empty array []")
            else:
                record(
                    "PASS",
                    "Data",
                    filename,
                    f"Valid JSON ({size:,} bytes, age {age.days}d)",
                )
        except json.JSONDecodeError as e:
            record("CRITICAL", "Data", filename, f"Invalid JSON: {e}")
        except Exception as e:
            record("HIGH", "Data", filename, f"Read error: {e}")

    # Check enrichment state specifically
    enrichment_path = DATA_DIR / "enrichment_state.json"
    if enrichment_path.exists():
        try:
            with open(enrichment_path, "r") as f:
                state = json.load(f)
            last_run = state.get("last_run") or state.get("last_enrichment")
            if last_run:
                print(f"  Enrichment last run: {last_run}")
        except Exception:
            pass

    # Check scraped data freshness
    scraped_files = list(DATA_DIR.glob("scraped_*.json"))
    for sf in scraped_files:
        stat = sf.stat()
        age = now - datetime.fromtimestamp(stat.st_mtime)
        name = sf.name
        if age > very_stale_threshold:
            record(
                "MEDIUM",
                "Data",
                name,
                f"Scraped data is {age.days} days old (>30 days)",
            )
        elif stat.st_size < 100:
            record(
                "HIGH",
                "Data",
                name,
                f"Scraped file suspiciously small ({stat.st_size} bytes)",
            )
        else:
            print(f"  {name:50s} {stat.st_size:>10,} bytes  age {age.days}d")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 7: SECURITY SCAN
# ═══════════════════════════════════════════════════════════════════════════════
def test_security() -> None:
    """Quick security scan of templates and API responses."""
    print("\n" + "=" * 70)
    print("TEST SUITE 7: SECURITY SCAN")
    print("=" * 70)

    # Scan templates for hardcoded secrets
    secret_patterns = [
        ("phx_", "PostHog API key"),
        ("sk-", "OpenAI/Anthropic API key"),
        ("sk_live_", "Stripe live key"),
        ("rnd_", "Render API key"),
        ("ghp_", "GitHub token"),
        ("sntrys_", "Sentry token"),
        ("xoxb-", "Slack bot token"),
        ("ADMIN_API_KEY", "Admin key reference"),
        ("Chandel13", "Hardcoded admin key"),
    ]

    print("  Scanning templates for hardcoded secrets...")
    templates = list(TEMPLATES_DIR.glob("*.html"))
    for template in templates:
        try:
            content = template.read_text(encoding="utf-8")
            for pattern, desc in secret_patterns:
                if pattern in content:
                    # Check if it's in a comment or actual code
                    # Filter out false positives like CSS class names
                    if pattern == "sk-" and "skill-" in content:
                        continue
                    record(
                        "CRITICAL",
                        "Security",
                        f"Template: {template.name}",
                        f"Found {desc} pattern '{pattern}' in template",
                    )
                    print(f"    [CRITICAL] {template.name}: found '{pattern}' ({desc})")
        except Exception:
            pass

    # Scan Python source for hardcoded secrets
    print("  Scanning Python source for hardcoded secrets...")
    py_files = list(LOCAL_PROJECT.glob("*.py"))
    for pyf in py_files:
        try:
            content = pyf.read_text(encoding="utf-8")
            # Check for hardcoded Chandel13
            if '"Chandel13"' in content or "'Chandel13'" in content:
                record(
                    "CRITICAL",
                    "Security",
                    f"Python: {pyf.name}",
                    "Hardcoded admin key 'Chandel13' found",
                )
                print(f"    [CRITICAL] {pyf.name}: hardcoded 'Chandel13'")
            # Check for inline API keys
            for pattern, desc in secret_patterns:
                if pattern == "Chandel13":
                    continue  # Already checked
                if pattern == "sk-" and "skip" in pyf.name:
                    continue
                # Look for actual key values (pattern followed by alphanumeric)
                import re

                matches = re.findall(
                    rf'["\']({re.escape(pattern)}[A-Za-z0-9_-]{{10,}})["\']', content
                )
                for match in matches:
                    record(
                        "CRITICAL",
                        "Security",
                        f"Python: {pyf.name}",
                        f"Hardcoded {desc}: {match[:15]}...",
                    )
                    print(f"    [CRITICAL] {pyf.name}: hardcoded {desc}")
        except Exception:
            pass

    # Test CSRF protection works
    print("  Testing CSRF protection...")
    # Try POST without CSRF token
    status, body, elapsed = http_post("/api/chat", {"message": "test"}, timeout=15)
    if status == 403:
        record(
            "PASS",
            "Security",
            "CSRF Protection",
            "POST without CSRF correctly rejected (403)",
        )
        print("    CSRF protection: ACTIVE (403 on missing token)")
    elif status == 200:
        record(
            "CRITICAL",
            "Security",
            "CSRF Protection",
            "POST succeeded without CSRF token!",
        )
        print("    [CRITICAL] CSRF protection: BYPASSED!")
    else:
        record(
            "MEDIUM",
            "Security",
            "CSRF Protection",
            f"Unexpected status {status} on CSRF test",
        )

    # Test directory traversal protection
    print("  Testing directory traversal...")
    status, body, elapsed = http_get("/static/../app.py")
    if status == 403:
        record(
            "PASS",
            "Security",
            "Path Traversal",
            "Directory traversal correctly blocked (403)",
        )
    elif status == 200:
        record(
            "CRITICAL",
            "Security",
            "Path Traversal",
            "Directory traversal NOT blocked - source code exposed!",
        )
    else:
        record(
            "PASS",
            "Security",
            "Path Traversal",
            f"Directory traversal returned {status}",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# REPORT GENERATION
# ═══════════════════════════════════════════════════════════════════════════════
def generate_report() -> None:
    """Generate the comprehensive stress test report."""
    print("\n\n" + "=" * 70)
    print("COMPREHENSIVE STRESS TEST REPORT")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target: {BASE_URL}")
    print("=" * 70)

    # Count by severity
    critical = [f for f in findings if f["severity"] == "CRITICAL"]
    high = [f for f in findings if f["severity"] == "HIGH"]
    medium = [f for f in findings if f["severity"] == "MEDIUM"]
    low = [f for f in findings if f["severity"] == "LOW"]
    passed = [f for f in findings if f["severity"] == "PASS"]

    print(
        f"\nSUMMARY: {len(critical)} CRITICAL | {len(high)} HIGH | {len(medium)} MEDIUM | {len(low)} LOW | {len(passed)} PASS"
    )
    print(
        f"Total tests: {test_count} | Passed HTTP: {pass_count} | Failed HTTP: {fail_count}"
    )

    # Health score calculation
    total_non_pass = len(critical) + len(high) + len(medium) + len(low)
    total_findings = total_non_pass + len(passed)
    if total_findings > 0:
        # Weighted: CRITICAL=-10, HIGH=-5, MEDIUM=-2, LOW=-1, PASS=0
        penalty = len(critical) * 10 + len(high) * 5 + len(medium) * 2 + len(low) * 1
        max_penalty = total_findings * 10
        health_score = max(0, round(100 - (penalty / max_penalty * 100)))
    else:
        health_score = 100
    print(f"\nOVERALL HEALTH SCORE: {health_score}/100")

    if health_score >= 90:
        print("Rating: EXCELLENT")
    elif health_score >= 75:
        print("Rating: GOOD")
    elif health_score >= 60:
        print("Rating: FAIR")
    elif health_score >= 40:
        print("Rating: POOR")
    else:
        print("Rating: CRITICAL")

    # Detailed findings
    for severity, label, items in [
        ("CRITICAL", "CRITICAL ISSUES", critical),
        ("HIGH", "HIGH ISSUES", high),
        ("MEDIUM", "MEDIUM ISSUES", medium),
        ("LOW", "LOW ISSUES", low),
    ]:
        if items:
            print(f"\n{'─' * 70}")
            print(f"{label} ({len(items)})")
            print(f"{'─' * 70}")
            for i, f in enumerate(items, 1):
                rt = (
                    f"({f['response_time_ms']:.0f}ms)"
                    if f["response_time_ms"] > 0
                    else ""
                )
                sc = f"[{f['status_code']}]" if f["status_code"] > 0 else ""
                print(f"  {i}. [{f['category']}] {f['test']}")
                print(f"     {f['description']} {sc} {rt}")

    # Performance summary
    print(f"\n{'─' * 70}")
    print("PERFORMANCE SUMMARY")
    print(f"{'─' * 70}")
    perf_findings = sorted(
        [f for f in findings if f["response_time_ms"] > 0],
        key=lambda x: x["response_time_ms"],
        reverse=True,
    )
    if perf_findings:
        print(f"  Slowest endpoints:")
        for f in perf_findings[:10]:
            marker = ""
            if f["response_time_ms"] > 10000:
                marker = " [CRITICAL]"
            elif f["response_time_ms"] > 5000:
                marker = " [HIGH]"
            elif f["response_time_ms"] > 3000:
                marker = " [MEDIUM]"
            print(f"    {f['test']:50s} {f['response_time_ms']:>8.0f}ms{marker}")

    # Pass list summary
    print(f"\n{'─' * 70}")
    print(f"PASSED TESTS ({len(passed)})")
    print(f"{'─' * 70}")
    for f in passed[:30]:
        print(f"  [OK] {f['test']}: {f['description'][:80]}")
    if len(passed) > 30:
        print(f"  ... and {len(passed) - 30} more")

    print(f"\n{'=' * 70}")
    print(
        f"END OF REPORT | Score: {health_score}/100 | {len(critical)} CRITICAL | {len(high)} HIGH | {len(medium)} MEDIUM | {len(low)} LOW"
    )
    print(f"{'=' * 70}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 70)
    print("NOVA AI SUITE - COMPREHENSIVE STRESS TEST")
    print(f"Target: {BASE_URL}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Wake up Render (first request might be slow)
    print("\nWaking up server (Render cold start)...")
    wake_status, _, wake_time = http_get("/health", timeout=60)
    print(f"  Server wake: {wake_status} ({wake_time:.1f}s)")
    if wake_time > 30:
        record(
            "MEDIUM",
            "Performance",
            "Cold Start",
            f"Render cold start took {wake_time:.1f}s",
        )

    test_backend_apis()
    test_frontend_templates()
    test_media_plan_generator()
    test_nova_chatbot()
    test_integration_health()
    test_data_quality()
    test_security()
    generate_report()
