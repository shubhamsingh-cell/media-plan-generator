"""Unit tests for ``api_enrichment.fetch_ats_postings`` (Greenhouse/Ashby/Lever).

One multi-provider hiring-signal feed over three no-key public ATS APIs, each
normalised into a consistent envelope. These tests are stdlib-only, fast, and
never touch the live network: ``urllib.request.urlopen`` is mocked and the
in-process cache helpers are patched so state never leaks between tests.

Test groups:
    1. Normal payloads -- each provider's verified shape (greenhouse object,
       ashby object, lever array) normalises to the common envelope.
    2. Edge payloads -- job cap enforcement, blank/missing fields, unknown
       provider, blank board, wrong top-level type.
    3. Network errors -- URLError, HTTPError, timeout, bad JSON each return a
       clean error envelope (never raise).
    4. Return-shape contract + URL construction per provider.
"""

from __future__ import annotations

import io
import json
import sys
import urllib.error
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402


# ─── Fakes / fixtures ─────────────────────────────────────────────────────────


class _FakeUrlopenContext:
    """Minimal context-manager that mimics urllib.request.urlopen()."""

    def __init__(self, payload: bytes) -> None:
        self._buf = io.BytesIO(payload)

    def __enter__(self) -> "_FakeUrlopenContext":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def read(self) -> bytes:
        return self._buf.read()


@pytest.fixture(autouse=True)
def _no_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable the in-process cache so each test exercises the parse path."""
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, data: None)


def _greenhouse_payload() -> bytes:
    """A realistic Greenhouse boards-api response (?content=true shape).

    Two jobs in New York, one in Tokyo, to exercise the location histogram.
    """
    return json.dumps(
        {
            "meta": {"total": 3},
            "jobs": [
                {
                    "title": "AI Research Engineer",
                    "location": {"name": "New York, New York, USA"},
                    "absolute_url": "https://careers.example.com/1",
                    "departments": [{"name": "Dev Eng"}],
                },
                {
                    "title": "Backend Engineer",
                    "location": {"name": "New York, New York, USA"},
                    "absolute_url": "https://careers.example.com/2",
                    "departments": [{"name": "Platform"}],
                },
                {
                    "title": "Site Reliability Engineer",
                    "location": {"name": "Tokyo, Japan"},
                    "absolute_url": "https://careers.example.com/3",
                    "departments": [],
                },
            ],
        }
    ).encode("utf-8")


def _ashby_payload() -> bytes:
    """A realistic Ashby posting-api response with compensation present."""
    return json.dumps(
        {
            "apiVersion": "1",
            "jobs": [
                {
                    "title": "Engineering Manager, EU",
                    "location": "Remote - European Union",
                    "department": "Engineering",
                    "jobUrl": "https://jobs.ashbyhq.com/ashby/abc",
                    "applyUrl": "https://jobs.ashbyhq.com/ashby/abc/application",
                    "compensation": {
                        "compensationTierSummary": ("€76K – €185K • Offers Equity"),
                        "scrapeableCompensationSalarySummary": "€76K - €185K",
                    },
                },
                {
                    "title": "Product Designer",
                    "location": "Remote - US",
                    "department": "Design",
                    "jobUrl": "https://jobs.ashbyhq.com/ashby/def",
                    "compensation": None,
                },
            ],
        }
    ).encode("utf-8")


def _lever_payload() -> bytes:
    """A realistic Lever postings response (a top-level JSON array)."""
    return json.dumps(
        [
            {
                "text": "AbelsonTaylor Writer",
                "categories": {
                    "location": "Arlington, TX",
                    "department": "Customer Success",
                    "commitment": "Regular Full Time (Salary)",
                },
                "hostedUrl": "https://jobs.lever.co/leverdemo/abc",
            },
            {
                "text": "Account Executive",
                "categories": {
                    "location": "Arlington, TX",
                    "department": "Sales",
                },
                "hostedUrl": "https://jobs.lever.co/leverdemo/def",
            },
        ]
    ).encode("utf-8")


def _greenhouse_many_jobs_payload(n: int) -> bytes:
    """A Greenhouse payload with ``n`` jobs, to exercise the _ATS_MAX_JOBS cap."""
    jobs = [
        {
            "title": f"Engineer {i}",
            "location": {"name": "Remote"},
            "absolute_url": f"https://careers.example.com/{i}",
            "departments": [{"name": "Engineering"}],
        }
        for i in range(n)
    ]
    return json.dumps({"meta": {"total": n}, "jobs": jobs}).encode("utf-8")


# ─── 1. Normal payloads parse correctly (per provider) ────────────────────────


def test_greenhouse_normalises() -> None:
    """A Greenhouse object normalises into the common envelope."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_greenhouse_payload()),
    ):
        out = api_enrichment.fetch_ats_postings("greenhouse", "datadog")

    assert "error" not in out, f"unexpected error: {out.get('error')}"
    assert out["source"] == "ATS:greenhouse"
    assert out["provider"] == "greenhouse"
    assert out["board"] == "datadog"
    assert out["job_count"] == 3
    assert out["total_available"] == 3
    first = out["jobs"][0]
    assert first["title"] == "AI Research Engineer"
    assert first["location"] == "New York, New York, USA"
    assert first["department"] == "Dev Eng"
    assert first["url"] == "https://careers.example.com/1"
    assert first["compensation"] is None  # greenhouse public API has no comp
    # Location histogram counts both NY jobs.
    assert out["locations_summary"]["New York, New York, USA"] == 2
    assert out["locations_summary"]["Tokyo, Japan"] == 1


def test_ashby_normalises_with_compensation() -> None:
    """An Ashby object normalises; compensation summary is surfaced as a string."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_ashby_payload()),
    ):
        out = api_enrichment.fetch_ats_postings("ashby", "ashby")

    assert "error" not in out
    assert out["source"] == "ATS:ashby"
    assert out["job_count"] == 2
    first = out["jobs"][0]
    assert first["title"] == "Engineering Manager, EU"
    assert first["location"] == "Remote - European Union"
    assert first["department"] == "Engineering"
    assert first["url"] == "https://jobs.ashbyhq.com/ashby/abc"
    assert "76K" in first["compensation"]
    # Second job has no compensation block.
    assert out["jobs"][1]["compensation"] is None


def test_lever_normalises_from_array() -> None:
    """A Lever top-level array normalises into the common envelope."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_lever_payload()),
    ):
        out = api_enrichment.fetch_ats_postings("lever", "leverdemo")

    assert "error" not in out
    assert out["source"] == "ATS:lever"
    assert out["job_count"] == 2
    first = out["jobs"][0]
    assert first["title"] == "AbelsonTaylor Writer"
    assert first["location"] == "Arlington, TX"
    assert first["department"] == "Customer Success"
    assert first["url"] == "https://jobs.lever.co/leverdemo/abc"
    assert first["compensation"] is None
    assert out["locations_summary"]["Arlington, TX"] == 2


def test_provider_is_case_insensitive() -> None:
    """Provider matching is case-insensitive."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_greenhouse_payload()),
    ):
        out = api_enrichment.fetch_ats_postings("GreenHouse", "datadog")

    assert "error" not in out
    assert out["provider"] == "greenhouse"


def test_success_envelope_has_documented_keys() -> None:
    """Success dict carries every key the chatbot tool wires against."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_greenhouse_payload()),
    ):
        out = api_enrichment.fetch_ats_postings("greenhouse", "datadog")

    for key in (
        "source",
        "provider",
        "board",
        "job_count",
        "total_available",
        "jobs",
        "locations_summary",
        "url",
    ):
        assert key in out, f"missing key in success envelope: {key}"
    assert isinstance(out["jobs"], list)
    assert isinstance(out["locations_summary"], dict)


# ─── 2. Edge payloads ─────────────────────────────────────────────────────────


def test_jobs_capped_at_max() -> None:
    """More than _ATS_MAX_JOBS postings are capped; total_available is exact."""
    n = api_enrichment._ATS_MAX_JOBS + 25
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_greenhouse_many_jobs_payload(n)),
    ):
        out = api_enrichment.fetch_ats_postings("greenhouse", "datadog")

    assert out["job_count"] == api_enrichment._ATS_MAX_JOBS
    assert len(out["jobs"]) == api_enrichment._ATS_MAX_JOBS
    assert out["total_available"] == n


def test_unknown_provider_short_circuits() -> None:
    """An unknown provider returns a clean error without a network call."""
    with mock.patch("urllib.request.urlopen") as urlopen:
        out = api_enrichment.fetch_ats_postings("workday", "foo")

    urlopen.assert_not_called()
    assert "error" in out
    assert "Unknown provider" in out["error"]
    assert out["provider"] == "workday"
    assert out["jobs"] == []
    assert out["locations_summary"] == {}


def test_blank_board_short_circuits() -> None:
    """A blank board token returns a clean error without a network call."""
    with mock.patch("urllib.request.urlopen") as urlopen:
        out = api_enrichment.fetch_ats_postings("greenhouse", "  ")

    urlopen.assert_not_called()
    assert "error" in out
    assert "board token is required" in out["error"]
    assert out["source"] == "ATS:greenhouse"


def test_lever_wrong_type_returns_error() -> None:
    """A Lever response that is an object (not an array) is rejected cleanly."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(b'{"jobs": []}'),
    ):
        out = api_enrichment.fetch_ats_postings("lever", "leverdemo")

    assert "error" in out
    assert "not an array" in out["error"]
    assert out["jobs"] == []


def test_greenhouse_wrong_type_returns_error() -> None:
    """A Greenhouse response that is an array (not an object) is rejected."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(b"[]"),
    ):
        out = api_enrichment.fetch_ats_postings("greenhouse", "datadog")

    assert "error" in out
    assert "not an object" in out["error"]


def test_missing_fields_default_to_empty_strings() -> None:
    """Jobs with missing fields normalise to empty strings, never crash."""
    payload = json.dumps(
        {"jobs": [{"title": "Engineer"}]}  # no location/url/departments
    ).encode("utf-8")
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(payload),
    ):
        out = api_enrichment.fetch_ats_postings("greenhouse", "datadog")

    assert "error" not in out
    job = out["jobs"][0]
    assert job["title"] == "Engineer"
    assert job["location"] == ""
    assert job["department"] == ""
    assert job["url"] == ""
    # Blank locations are excluded from the histogram.
    assert out["locations_summary"] == {}


# ─── 3. Network errors ────────────────────────────────────────────────────────


def test_url_error_returns_error_envelope() -> None:
    """A urllib URLError yields a clean error dict, never raises."""
    with mock.patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.URLError("connection refused"),
    ):
        out = api_enrichment.fetch_ats_postings("greenhouse", "datadog")

    assert out["source"] == "ATS:greenhouse"
    assert "error" in out
    assert "Network error" in out["error"]
    assert out["jobs"] == []
    assert "url" in out


def test_http_error_returns_error_envelope() -> None:
    """An HTTPError (e.g. 404 unknown board) is captured into the envelope."""
    err = urllib.error.HTTPError(
        url="https://boards-api.greenhouse.io/v1/boards/nope/jobs",
        code=404,
        msg="Not Found",
        hdrs=None,  # type: ignore[arg-type]
        fp=io.BytesIO(b"not found"),
    )
    with mock.patch("urllib.request.urlopen", side_effect=err):
        out = api_enrichment.fetch_ats_postings("greenhouse", "nope")

    assert "error" in out
    assert "HTTP 404" in out["error"]
    assert out["jobs"] == []


def test_timeout_returns_error_envelope() -> None:
    """A socket timeout (OSError subclass) yields an error, not a crash."""
    with mock.patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
        out = api_enrichment.fetch_ats_postings("ashby", "ashby")

    assert "error" in out
    assert out["jobs"] == []
    assert out["source"] == "ATS:ashby"


def test_bad_json_returns_error_envelope() -> None:
    """Malformed JSON body is captured as a parse error, never raises."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(b"<html>not json</html>"),
    ):
        out = api_enrichment.fetch_ats_postings("lever", "leverdemo")

    assert "error" in out
    assert "Bad JSON" in out["error"]
    assert out["source"] == "ATS:lever"


# ─── 4. URL construction (per provider) ───────────────────────────────────────


def test_greenhouse_url_requests_content() -> None:
    """Greenhouse URL includes ?content=true (needed for departments)."""
    captured: List[str] = []

    def _capture(req: Any, *args: Any, **kwargs: Any) -> _FakeUrlopenContext:
        captured.append(req.full_url if hasattr(req, "full_url") else str(req))
        return _FakeUrlopenContext(_greenhouse_payload())

    with mock.patch("urllib.request.urlopen", side_effect=_capture):
        api_enrichment.fetch_ats_postings("greenhouse", "datadog")

    assert captured, "urlopen was not called"
    url = captured[0]
    assert "boards-api.greenhouse.io/v1/boards/datadog/jobs" in url
    assert "content=true" in url


def test_lever_url_requests_json_mode() -> None:
    """Lever URL includes ?mode=json."""
    captured: List[str] = []

    def _capture(req: Any, *args: Any, **kwargs: Any) -> _FakeUrlopenContext:
        captured.append(req.full_url if hasattr(req, "full_url") else str(req))
        return _FakeUrlopenContext(_lever_payload())

    with mock.patch("urllib.request.urlopen", side_effect=_capture):
        api_enrichment.fetch_ats_postings("lever", "leverdemo")

    assert captured, "urlopen was not called"
    url = captured[0]
    assert "api.lever.co/v0/postings/leverdemo" in url
    assert "mode=json" in url


def test_ashby_url_requests_compensation() -> None:
    """Ashby URL includes ?includeCompensation=true."""
    captured: List[str] = []

    def _capture(req: Any, *args: Any, **kwargs: Any) -> _FakeUrlopenContext:
        captured.append(req.full_url if hasattr(req, "full_url") else str(req))
        return _FakeUrlopenContext(_ashby_payload())

    with mock.patch("urllib.request.urlopen", side_effect=_capture):
        api_enrichment.fetch_ats_postings("ashby", "ashby")

    assert captured, "urlopen was not called"
    url = captured[0]
    assert "api.ashbyhq.com/posting-api/job-board/ashby" in url
    assert "includeCompensation=true" in url
