"""Unit tests for ``api_enrichment.fetch_uk_ons_data`` (ONS Beta migration).

The function was migrated from the retired ``api.ons.gov.uk/timeseries`` host
to the ONS Beta API ``api.beta.ons.gov.uk/v1/data?uri=...`` which still serves
the legacy timeseries shape (parallel ``months``/``quarters``/``years`` arrays
plus a ``description`` block). These tests are stdlib-only, fast, and never
touch the live network: ``urllib.request.urlopen`` is mocked and the in-process
cache helpers are patched so state never leaks between tests.

Test groups:
    1. Normal payload -- the verified ONS shape parses to a clean dict with the
       latest non-blank observation, recent window, unit, and CDID.
    2. Edge payloads -- empty arrays, quarterly fallback, unknown dataset.
    3. Network errors -- URLError, HTTPError, timeout, bad JSON each return a
       clean error envelope (never raise).
    4. Return-shape contract -- success and failure envelopes carry the
       documented keys so the chatbot tool can be wired against them.
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


def _normal_payload() -> bytes:
    """A realistic ONS Beta unemployment (MGSX) response, trimmed but exact.

    Modeled on the live 2026-06-03 payload: monthly/quarterly/annual arrays of
    {date, value (string), label, ...} plus a ``description`` block with
    title/unit/cdid. Values are STRINGS, matching the real API.
    """
    return json.dumps(
        {
            "type": "timeseries",
            "uri": (
                "/employmentandlabourmarket/peoplenotinwork/"
                "unemployment/timeseries/mgsx/lms"
            ),
            "description": {
                "title": "Unemployment rate (aged 16 and over, seasonally adjusted): %",
                "unit": "%",
                "cdid": "MGSX",
                "preUnit": "",
            },
            "months": [
                {"date": "2025 DEC", "value": "5.2", "label": "2025 NOV-JAN"},
                {"date": "2026 JAN", "value": "4.9", "label": "2025 DEC-FEB"},
                {"date": "2026 FEB", "value": "5.0", "label": "2026 JAN-MAR"},
            ],
            "quarters": [
                {"date": "2025 Q4", "value": "4.9", "label": "2025 Q4"},
                {"date": "2026 Q1", "value": "5.0", "label": "2026 Q1"},
            ],
            "years": [
                {"date": "2024", "value": "4.3", "label": "2024"},
                {"date": "2025", "value": "4.8", "label": "2025"},
            ],
        }
    ).encode("utf-8")


def _trailing_blank_payload() -> bytes:
    """A response whose latest monthly value is blank ("") not numeric.

    Verifies the parser walks backward to the last NON-BLANK observation
    rather than blindly taking the final array element.
    """
    return json.dumps(
        {
            "description": {
                "title": "Employment rate (aged 16 to 64, seasonally adjusted): %",
                "unit": "%",
                "cdid": "LF24",
            },
            "months": [
                {"date": "2026 JAN", "value": "75.1", "label": "..."},
                {"date": "2026 FEB", "value": "75.0", "label": "..."},
                {"date": "2026 MAR", "value": "", "label": "..."},
            ],
            "quarters": [],
            "years": [],
        }
    ).encode("utf-8")


def _quarterly_only_payload() -> bytes:
    """A response with no monthly data; the parser should fall back to quarters."""
    return json.dumps(
        {
            "description": {
                "title": "UK Vacancies (thousands) - Total",
                "unit": "",
                "cdid": "AP2Y",
            },
            "months": [],
            "quarters": [
                {"date": "2025 Q4", "value": "725", "label": "2025 Q4"},
                {"date": "2026 Q1", "value": "712", "label": "2026 Q1"},
            ],
            "years": [{"date": "2025", "value": "748", "label": "2025"}],
        }
    ).encode("utf-8")


def _empty_series_payload() -> bytes:
    """A well-formed response with no observations at all."""
    return json.dumps(
        {
            "description": {"title": "Empty", "unit": "%", "cdid": "MGSX"},
            "months": [],
            "quarters": [],
            "years": [],
        }
    ).encode("utf-8")


# ─── 1. Normal payload parses correctly ───────────────────────────────────────


def test_normal_payload_parses_latest_and_recent() -> None:
    """A standard ONS doc yields the latest non-blank obs + recent window."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_normal_payload()),
    ):
        out = api_enrichment.fetch_uk_ons_data("unemployment")

    assert "error" not in out, f"unexpected error: {out.get('error')}"
    assert out["source"] == "uk_ons"
    assert out["tool"] == "fetch_uk_ons_data"
    assert out["dataset"] == "unemployment"
    assert out["cdid"] == "MGSX"
    assert out["series_id"] == "MGSX"  # legacy alias preserved
    assert out["unit"] == "%"
    # Latest non-blank = the final monthly element here (2026 FEB, 5.0 as float).
    assert out["latest_value"] == 5.0
    assert isinstance(out["latest_value"], float)
    assert out["latest_period"] == "2026 FEB"
    # total_observations counts months + quarters + years (3 + 2 + 2).
    assert out["total_observations"] == 7
    # Recent window is oldest->newest, capped, raw string values preserved.
    assert out["recent_monthly"][0] == {
        "period": "2025 DEC",
        "value": "5.2",
        "label": "2025 NOV-JAN",
    }
    assert out["recent_monthly"][-1]["period"] == "2026 FEB"
    assert len(out["recent_monthly"]) <= api_enrichment._UK_ONS_RECENT_LIMIT


def test_default_dataset_is_employment() -> None:
    """Calling with no args targets the employment series."""
    captured: List[str] = []

    def _capture(req: Any, *args: Any, **kwargs: Any) -> _FakeUrlopenContext:
        captured.append(req.full_url if hasattr(req, "full_url") else str(req))
        return _FakeUrlopenContext(_normal_payload())

    with mock.patch("urllib.request.urlopen", side_effect=_capture):
        out = api_enrichment.fetch_uk_ons_data()

    assert out["dataset"] == "employment"
    assert captured, "urlopen was not called"
    # LF24 is the employment CDID; it must appear in the request URL.
    assert "lf24" in captured[0].lower()


def test_trailing_blank_picks_last_non_blank() -> None:
    """Parser walks back past a blank trailing value to the real latest obs."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_trailing_blank_payload()),
    ):
        out = api_enrichment.fetch_uk_ons_data("employment")

    assert "error" not in out
    assert out["latest_value"] == 75.0
    assert out["latest_period"] == "2026 FEB"


def test_quarterly_fallback_when_no_months() -> None:
    """When monthly obs are absent, the parser falls back to quarterly data."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_quarterly_only_payload()),
    ):
        out = api_enrichment.fetch_uk_ons_data("vacancies")

    assert "error" not in out
    assert out["latest_value"] == 712.0
    assert out["latest_period"] == "2026 Q1"
    assert out["recent_monthly"][0]["period"] == "2025 Q4"


def test_success_envelope_has_documented_keys() -> None:
    """Success dict carries every key the chatbot tool wires against."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_normal_payload()),
    ):
        out = api_enrichment.fetch_uk_ons_data("unemployment")

    for key in (
        "source",
        "tool",
        "dataset",
        "cdid",
        "series_id",
        "description",
        "unit",
        "latest_value",
        "latest_period",
        "recent_monthly",
        "total_observations",
        "url",
    ):
        assert key in out, f"missing key in success envelope: {key}"
    assert isinstance(out["recent_monthly"], list)


# ─── 2. Edge payloads ─────────────────────────────────────────────────────────


def test_empty_series_yields_null_latest() -> None:
    """A response with no observations parses without error, latest = None."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_empty_series_payload()),
    ):
        out = api_enrichment.fetch_uk_ons_data("unemployment")

    assert "error" not in out
    assert out["latest_value"] is None
    assert out["latest_period"] is None
    assert out["recent_monthly"] == []
    assert out["total_observations"] == 0


def test_unknown_dataset_short_circuits() -> None:
    """An unknown dataset returns a clean error without a network call."""
    with mock.patch("urllib.request.urlopen") as urlopen:
        out = api_enrichment.fetch_uk_ons_data("bogus")

    urlopen.assert_not_called()
    assert "error" in out
    assert "Unknown dataset" in out["error"]
    assert out["source"] == "uk_ons"
    assert out["tool"] == "fetch_uk_ons_data"
    assert out["recent_monthly"] == []


# ─── 3. Network errors ────────────────────────────────────────────────────────


def test_url_error_returns_error_envelope() -> None:
    """A urllib URLError yields a clean error dict, never raises."""
    with mock.patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.URLError("connection refused"),
    ):
        out = api_enrichment.fetch_uk_ons_data("unemployment")

    assert out["source"] == "uk_ons"
    assert "error" in out
    assert "Network error" in out["error"]
    assert out["latest_value"] is None
    assert "url" in out  # echoed back for debuggability


def test_http_error_returns_error_envelope() -> None:
    """An HTTPError (e.g. 404 — the symptom that triggered this migration)."""
    err = urllib.error.HTTPError(
        url="https://api.beta.ons.gov.uk/v1/data?uri=bad",
        code=404,
        msg="Not Found",
        hdrs=None,  # type: ignore[arg-type]
        fp=io.BytesIO(b"not found"),
    )
    with mock.patch("urllib.request.urlopen", side_effect=err):
        out = api_enrichment.fetch_uk_ons_data("unemployment")

    assert "error" in out
    assert "HTTP 404" in out["error"]
    assert out["latest_value"] is None


def test_timeout_returns_error_envelope() -> None:
    """A socket timeout (OSError subclass) yields an error, not a crash."""
    with mock.patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
        out = api_enrichment.fetch_uk_ons_data("unemployment")

    assert "error" in out
    assert out["latest_value"] is None
    assert out["source"] == "uk_ons"


def test_bad_json_returns_error_envelope() -> None:
    """Malformed JSON body is captured as a parse error, never raises."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(b"<html>not json</html>"),
    ):
        out = api_enrichment.fetch_uk_ons_data("unemployment")

    assert "error" in out
    assert "Bad JSON" in out["error"]
    assert out["source"] == "uk_ons"


# ─── 4. URL construction ──────────────────────────────────────────────────────


def test_url_targets_ons_beta_host() -> None:
    """The request URL targets the ONS Beta host, not the retired api.ons.gov.uk."""
    captured: List[str] = []

    def _capture(req: Any, *args: Any, **kwargs: Any) -> _FakeUrlopenContext:
        captured.append(req.full_url if hasattr(req, "full_url") else str(req))
        return _FakeUrlopenContext(_normal_payload())

    with mock.patch("urllib.request.urlopen", side_effect=_capture):
        api_enrichment.fetch_uk_ons_data("unemployment")

    assert captured, "urlopen was not called"
    url = captured[0]
    assert "api.beta.ons.gov.uk" in url
    assert "api.ons.gov.uk/timeseries" not in url
    assert "mgsx" in url.lower()
