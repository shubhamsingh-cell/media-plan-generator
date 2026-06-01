"""Unit tests for ``api_enrichment.fetch_dbnomics_series``.

These tests are stdlib-only, fast, and never touch the live network. The
HTTP call is mocked at ``urllib.request.urlopen`` with a minimal
context-manager fake (mirroring the pattern in ``test_s50_upgrades.py``), and
the in-process cache helpers (``_get_cached`` / ``_set_cached``) are patched so
caching never leaks state between tests.

Test groups:
    1. Normal payload -- verified DBnomics shape parses to a clean dict with
       latest non-null observation, recent window, unit, and series name.
    2. Edge payloads -- empty docs, all-null values, missing args.
    3. Network errors -- URLError, HTTPError, timeout each return a clean
       error envelope (never raise).
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
    """Disable the in-process cache so each test exercises the parse path.

    Without this, a cached success from one test could short-circuit another
    test's mocked urlopen and hide regressions.
    """
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, data: None)


def _normal_payload() -> bytes:
    """A realistic DBnomics IMF-WEO response (USA.LUR), trimmed but shape-exact.

    Modeled on the live 2026-06-02 payload: parallel ``period``/``value``
    arrays, ``dimensions.unit`` code plus a ``dataset.dimensions_values_labels``
    label map, and forecast years included.
    """
    return json.dumps(
        {
            "_meta": {},
            "provider": {"code": "IMF"},
            "dataset": {
                "code": "WEO:2025-04",
                "name": "World Economic Outlook by countries",
                "dimensions_values_labels": {
                    "unit": {"pcent_total_labor_force": "Percent of total labor force"}
                },
            },
            "errors": [],
            "series": {
                "num_found": 1,
                "docs": [
                    {
                        "@frequency": "annual",
                        "provider_code": "IMF",
                        "dataset_code": "WEO:2025-04",
                        "dataset_name": "World Economic Outlook by countries",
                        "series_code": "USA.LUR.pcent_total_labor_force",
                        "series_name": (
                            "United States – Unemployment rate – "
                            "Percent of total labor force"
                        ),
                        "dimensions": {
                            "unit": "pcent_total_labor_force",
                            "weo-country": "USA",
                            "weo-subject": "LUR",
                        },
                        "period": ["2022", "2023", "2024", "2025", "2026"],
                        "value": [3.65, 3.633, 4.033, 4.159, 4.151],
                    }
                ],
            },
        }
    ).encode("utf-8")


def _empty_docs_payload() -> bytes:
    """A well-formed response with an empty ``series.docs`` array."""
    return json.dumps(
        {
            "provider": {"code": "IMF"},
            "dataset": {},
            "errors": ["Series not found"],
            "series": {"num_found": 0, "docs": []},
        }
    ).encode("utf-8")


def _all_null_payload() -> bytes:
    """A response whose single doc has only null observation values."""
    return json.dumps(
        {
            "provider": {"code": "IMF"},
            "dataset": {"name": "World Economic Outlook by countries"},
            "errors": [],
            "series": {
                "num_found": 1,
                "docs": [
                    {
                        "@frequency": "annual",
                        "provider_code": "IMF",
                        "dataset_code": "WEO:2025-04",
                        "series_code": "USA.LUR",
                        "series_name": "United States – Unemployment rate",
                        "dimensions": {"unit": "pcent_total_labor_force"},
                        "period": ["2024", "2025", "2026"],
                        "value": [None, None, None],
                    }
                ],
            },
        }
    ).encode("utf-8")


def _trailing_null_payload() -> bytes:
    """A doc whose latest value is null but earlier ones are present.

    Verifies the parser walks backward to the last NON-NULL observation
    rather than blindly taking the final array element.
    """
    return json.dumps(
        {
            "provider": {"code": "BLS"},
            "dataset": {"name": "CPI"},
            "errors": [],
            "series": {
                "num_found": 1,
                "docs": [
                    {
                        "@frequency": "monthly",
                        "provider_code": "BLS",
                        "dataset_code": "cu",
                        "series_code": "CUUR0000SA0",
                        "series_name": "U.S. city average – All items",
                        "dimensions": {},
                        "period": ["2025-01", "2025-02", "2025-03"],
                        "value": [315.6, 317.6, None],
                    }
                ],
            },
        }
    ).encode("utf-8")


# ─── 1. Normal payload parses correctly ───────────────────────────────────────


def test_normal_payload_parses_latest_and_recent() -> None:
    """A standard DBnomics doc yields the latest non-null obs + recent window."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_normal_payload()),
    ):
        out = api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "USA.LUR")

    assert "error" not in out, f"unexpected error: {out.get('error')}"
    assert out["source"] == "DBnomics"
    assert out["provider"] == "IMF"
    assert out["series_name"].startswith("United States")
    # Latest non-null = the final element here (2026, 4.151).
    assert out["latest_value"] == 4.151
    assert out["latest_period"] == "2026"
    assert out["observation_count"] == 5
    # Recent window is oldest->newest and capped by _DBNOMICS_RECENT_LIMIT.
    assert out["recent"][0] == {"period": "2022", "value": 3.65}
    assert out["recent"][-1] == {"period": "2026", "value": 4.151}
    assert len(out["recent"]) <= api_enrichment._DBNOMICS_RECENT_LIMIT


def test_normal_payload_resolves_unit_label() -> None:
    """Unit is resolved from dimensions.unit via the dataset label map."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_normal_payload()),
    ):
        out = api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "USA.LUR")

    assert out["unit"] == "Percent of total labor force"
    assert out["frequency"] == "annual"


def test_trailing_null_picks_last_non_null() -> None:
    """Parser walks back past a trailing null to the real latest observation."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_trailing_null_payload()),
    ):
        out = api_enrichment.fetch_dbnomics_series("BLS", "cu", "CUUR0000SA0")

    assert "error" not in out
    assert out["latest_value"] == 317.6
    assert out["latest_period"] == "2025-02"
    # The trailing null is excluded from the recent window.
    assert all(obs["value"] is not None for obs in out["recent"])


def test_success_envelope_has_documented_keys() -> None:
    """Success dict carries every key the chatbot tool wires against."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_normal_payload()),
    ):
        out = api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "USA.LUR")

    for key in (
        "source",
        "provider",
        "dataset",
        "series",
        "series_name",
        "latest_value",
        "latest_period",
        "recent",
        "unit",
        "url",
    ):
        assert key in out, f"missing key in success envelope: {key}"
    assert isinstance(out["recent"], list)


# ─── 2. Edge payloads ─────────────────────────────────────────────────────────


def test_empty_docs_returns_error_envelope() -> None:
    """An empty docs array returns a clean error dict, never raises."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_empty_docs_payload()),
    ):
        out = api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "BOGUS")

    assert out["source"] == "DBnomics"
    assert "error" in out
    assert "No series found" in out["error"]
    assert out["latest_value"] is None
    assert out["latest_period"] is None
    assert out["recent"] == []


def test_all_null_values_returns_error_envelope() -> None:
    """A doc with only null values returns an error, not a bogus latest."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(_all_null_payload()),
    ):
        out = api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "USA.LUR")

    assert "error" in out
    assert "null" in out["error"].lower()
    assert out["latest_value"] is None
    assert out["recent"] == []


def test_missing_arguments_short_circuit() -> None:
    """Blank provider/dataset/series fail fast without a network call."""
    with mock.patch("urllib.request.urlopen") as urlopen:
        out = api_enrichment.fetch_dbnomics_series("", "WEO:latest", "USA.LUR")

    urlopen.assert_not_called()
    assert "error" in out
    assert "required" in out["error"]
    assert out["source"] == "DBnomics"


# ─── 3. Network errors ────────────────────────────────────────────────────────


def test_url_error_returns_error_envelope() -> None:
    """A urllib URLError yields a clean error dict, never raises."""
    with mock.patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.URLError("connection refused"),
    ):
        out = api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "USA.LUR")

    assert out["source"] == "DBnomics"
    assert "error" in out
    assert "Network error" in out["error"]
    assert out["latest_value"] is None
    # URL is echoed back on failure for debuggability.
    assert "url" in out


def test_http_error_returns_error_envelope() -> None:
    """An HTTPError (e.g. 404) is captured into the error envelope."""
    err = urllib.error.HTTPError(
        url="https://api.db.nomics.world/v22/series/IMF/WEO:latest/USA.LUR",
        code=404,
        msg="Not Found",
        hdrs=None,  # type: ignore[arg-type]
        fp=io.BytesIO(b"not found"),
    )
    with mock.patch("urllib.request.urlopen", side_effect=err):
        out = api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "USA.LUR")

    assert "error" in out
    assert "HTTP 404" in out["error"]
    assert out["latest_value"] is None


def test_timeout_returns_error_envelope() -> None:
    """A socket timeout (OSError subclass) yields an error, not a crash."""
    with mock.patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
        out = api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "USA.LUR")

    assert "error" in out
    assert out["latest_value"] is None
    assert out["source"] == "DBnomics"


def test_bad_json_returns_error_envelope() -> None:
    """Malformed JSON body is captured as a parse error, never raises."""
    with mock.patch(
        "urllib.request.urlopen",
        return_value=_FakeUrlopenContext(b"<html>not json</html>"),
    ):
        out = api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "USA.LUR")

    assert "error" in out
    assert "Bad JSON" in out["error"]
    assert out["source"] == "DBnomics"


# ─── 4. URL construction ──────────────────────────────────────────────────────


def test_url_preserves_colon_and_dot_in_codes() -> None:
    """Dataset/series codes keep ':' and '.' (DBnomics requires them)."""
    captured: List[str] = []

    def _capture(req: Any, *args: Any, **kwargs: Any) -> _FakeUrlopenContext:
        captured.append(req.full_url if hasattr(req, "full_url") else str(req))
        return _FakeUrlopenContext(_normal_payload())

    with mock.patch("urllib.request.urlopen", side_effect=_capture):
        api_enrichment.fetch_dbnomics_series("IMF", "WEO:latest", "USA.LUR")

    assert captured, "urlopen was not called"
    url = captured[0]
    assert "IMF/WEO:latest/USA.LUR" in url
    assert url.endswith("?observations=1")
