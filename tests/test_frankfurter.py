"""Unit tests for ``api_enrichment.fetch_fx_rates`` (Frankfurter FX).

Frankfurter exposes the ECB's daily reference rates with no API key. These
tests are stdlib-only, fast, and never touch the live network:
``urllib.request.urlopen`` is mocked and the in-process cache helpers are
patched so state never leaks between tests.

Test groups:
    1. Normal payload -- the verified Frankfurter shape parses to a clean dict
       with float rates, the ECB date, and an echo of requested symbols.
    2. Edge payloads -- empty rates, non-numeric rate coercion, blank base.
    3. Network errors -- URLError, HTTPError (404 invalid base), timeout, bad
       JSON each return a clean error envelope (never raise).
    4. Return-shape contract + URL construction.
"""

from __future__ import annotations

import io
import json
import sys
import urllib.error
from pathlib import Path
from typing import Any, List
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
    """A realistic Frankfurter response (USD base, 3 symbols).

    Modeled on the live 2026-06-03 payload: {amount, base, date, rates{}}.
    """
    return json.dumps(
        {
            "amount": 1.0,
            "base": "USD",
            "date": "2026-06-02",
            "rates": {"EUR": 0.85844, "GBP": 0.74225, "INR": 95.27},
        }
    ).encode("utf-8")


def _mixed_types_payload() -> bytes:
    """A response whose rates include a non-numeric junk value.

    Verifies defensive float coercion drops bad entries without crashing.
    """
    return json.dumps(
        {
            "amount": 1.0,
            "base": "USD",
            "date": "2026-06-02",
            "rates": {"EUR": 0.858, "GBP": "0.742", "JUNK": "n/a"},
        }
    ).encode("utf-8")


def _empty_rates_payload() -> bytes:
    """A well-formed response carrying an empty rates object."""
    return json.dumps(
        {"amount": 1.0, "base": "USD", "date": "2026-06-02", "rates": {}}
    ).encode("utf-8")


# ─── 1. Normal payload parses correctly ───────────────────────────────────────


def test_normal_payload_parses_rates() -> None:
    """A standard Frankfurter doc yields float rates and the ECB date."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_normal_payload()),
    ):
        out = api_enrichment.fetch_fx_rates("USD", "EUR,GBP,INR")

    assert "error" not in out, f"unexpected error: {out.get('error')}"
    assert out["source"] == "Frankfurter"
    assert out["base"] == "USD"
    assert out["date"] == "2026-06-02"
    assert out["rates"] == {"EUR": 0.85844, "GBP": 0.74225, "INR": 95.27}
    assert all(isinstance(v, float) for v in out["rates"].values())
    assert out["rate_count"] == 3
    assert out["symbols"] == ["EUR", "GBP", "INR"]


def test_symbols_are_normalised_and_deduped() -> None:
    """Lower-case, whitespace, and duplicate symbols are cleaned up."""
    captured: List[str] = []

    def _capture(req: Any, *args: Any, **kwargs: Any) -> _FakeUrlopenContext:
        captured.append(req.full_url if hasattr(req, "full_url") else str(req))
        return _FakeUrlopenContext(_normal_payload())

    with mock.patch("urllib.request.urlopen", side_effect=_capture):
        out = api_enrichment.fetch_fx_rates("usd", " eur , gbp , eur ")

    # 'eur' duplicate removed; order preserved; all upper-cased.
    assert out["symbols"] == ["EUR", "GBP"]
    assert out["base"] == "USD"
    assert captured, "urlopen was not called"
    assert "symbols=EUR%2CGBP" in captured[0]


def test_non_numeric_rates_are_dropped() -> None:
    """Junk rate values are coerced/dropped; valid ones survive as floats."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_mixed_types_payload()),
    ):
        out = api_enrichment.fetch_fx_rates("USD")

    assert "error" not in out
    assert out["rates"]["EUR"] == 0.858
    assert out["rates"]["GBP"] == 0.742  # string "0.742" coerced to float
    assert "JUNK" not in out["rates"]  # non-numeric dropped
    assert out["rate_count"] == 2


def test_success_envelope_has_documented_keys() -> None:
    """Success dict carries every key the chatbot tool wires against."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_normal_payload()),
    ):
        out = api_enrichment.fetch_fx_rates("USD", "EUR")

    for key in (
        "source",
        "base",
        "date",
        "rates",
        "rate_count",
        "symbols",
        "url",
    ):
        assert key in out, f"missing key in success envelope: {key}"
    assert isinstance(out["rates"], dict)


# ─── 2. Edge payloads ─────────────────────────────────────────────────────────


def test_empty_rates_returns_error_envelope() -> None:
    """An empty rates object returns a clean error, not a bogus success."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_empty_rates_payload()),
    ):
        out = api_enrichment.fetch_fx_rates("USD", "ZZZ")

    assert "error" in out
    assert "No rates" in out["error"]
    assert out["rates"] == {}
    assert out["source"] == "Frankfurter"


def test_default_base_is_usd() -> None:
    """Calling with no args defaults the base to USD."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_normal_payload()),
    ):
        out = api_enrichment.fetch_fx_rates()

    assert out["base"] == "USD"


# ─── 3. Network errors ────────────────────────────────────────────────────────


def test_url_error_returns_error_envelope() -> None:
    """A urllib URLError yields a clean error dict, never raises."""
    with mock.patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.URLError("connection refused"),
    ):
        out = api_enrichment.fetch_fx_rates("USD", "EUR")

    assert out["source"] == "Frankfurter"
    assert "error" in out
    assert "Network error" in out["error"]
    assert out["rates"] == {}
    assert "url" in out


def test_http_error_invalid_base_returns_error_envelope() -> None:
    """An unknown base currency returns HTTP 404 -> clean error envelope.

    This mirrors the real Frankfurter behaviour: base=ZZZ -> 404
    {"message":"not found"}.
    """
    err = urllib.error.HTTPError(
        url="https://api.frankfurter.dev/v1/latest?base=ZZZ",
        code=404,
        msg="Not Found",
        hdrs=None,  # type: ignore[arg-type]
        fp=io.BytesIO(b'{"message":"not found"}'),
    )
    with mock.patch("urllib.request.urlopen", side_effect=err):
        out = api_enrichment.fetch_fx_rates("ZZZ")

    assert "error" in out
    assert "HTTP 404" in out["error"]
    assert out["rates"] == {}
    assert out["base"] == "ZZZ"


def test_timeout_returns_error_envelope() -> None:
    """A socket timeout (OSError subclass) yields an error, not a crash."""
    with mock.patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
        out = api_enrichment.fetch_fx_rates("USD")

    assert "error" in out
    assert out["rates"] == {}
    assert out["source"] == "Frankfurter"


def test_bad_json_returns_error_envelope() -> None:
    """Malformed JSON body is captured as a parse error, never raises."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(b"<html>not json</html>"),
    ):
        out = api_enrichment.fetch_fx_rates("USD")

    assert "error" in out
    assert "Bad JSON" in out["error"]
    assert out["source"] == "Frankfurter"


# ─── 4. URL construction ──────────────────────────────────────────────────────


def test_url_targets_frankfurter_dev_host() -> None:
    """The request URL targets the .dev host with the base param."""
    captured: List[str] = []

    def _capture(req: Any, *args: Any, **kwargs: Any) -> _FakeUrlopenContext:
        captured.append(req.full_url if hasattr(req, "full_url") else str(req))
        return _FakeUrlopenContext(_normal_payload())

    with mock.patch("urllib.request.urlopen", side_effect=_capture):
        api_enrichment.fetch_fx_rates("USD", "EUR,GBP")

    assert captured, "urlopen was not called"
    url = captured[0]
    assert "api.frankfurter.dev/v1/latest" in url
    assert "base=USD" in url


def test_no_symbols_omits_symbols_param() -> None:
    """Calling without symbols requests all currencies (no symbols= param)."""
    captured: List[str] = []

    def _capture(req: Any, *args: Any, **kwargs: Any) -> _FakeUrlopenContext:
        captured.append(req.full_url if hasattr(req, "full_url") else str(req))
        return _FakeUrlopenContext(_normal_payload())

    with mock.patch("urllib.request.urlopen", side_effect=_capture):
        api_enrichment.fetch_fx_rates("EUR")

    assert captured, "urlopen was not called"
    assert "symbols=" not in captured[0]
    assert "base=EUR" in captured[0]
