"""Tests for the NAICS 2022 industry typeahead (S94, Jesse Ofner feedback).

The wizard's Industry step only offered the 21 curated internal industry
keys (shared_utils.INDUSTRY_LABEL_MAP), which the whole benchmark stack
keys off. This adds a searchable typeahead over the full US NAICS 2022
index (data/naics_2022.json, 2,125 codes) WITHOUT changing those 21 keys.

Covers:
    1. Data-contract invariant -- every code in data/naics_2022.json
       resolves (via naics_lookup.resolve_internal_key) to a key that
       exists in shared_utils.INDUSTRY_LABEL_MAP. This is the guarantee
       the whole feature depends on: a NAICS pick must always map to a
       working internal key, never an orphan.
    2. naics_search() ranking -- numeric code-prefix match, multi-token
       text match, no-match, and the `limit` cap.
    3. format_industry_label() -- the single-source label suffix helper
       used by both gen_data paths in app.py and routes/export.py, incl.
       the empty-string-safe "missing naics -> unchanged label" contract.
    4. /api/naics/search live HTTP smoke test (200 shape + 400 on missing
       q), using the same in-process app.ThreadedHTTPServer fixture
       pattern as tests/test_api_estimate.py / test_generate_concurrency.py.
    5. Plan-payload round-trip -- plan_schema.PlanData preserves
       naics_selected_code/title (via its non-lossy `extra` dict) when
       present, and plans without them behave identically to today.
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
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module  # noqa: E402
import naics_lookup  # noqa: E402
from shared_utils import INDUSTRY_LABEL_MAP, format_industry_label  # noqa: E402
from plan_schema import PlanData  # noqa: E402


# ═══════════════════════════════════════════════════════════════════════
# 1. Data-contract invariant
# ═══════════════════════════════════════════════════════════════════════


class TestEveryCodeResolves:
    def test_dataset_loaded(self) -> None:
        assert naics_lookup.is_loaded(), "data/naics_2022.json failed to load"
        assert len(naics_lookup._CODES) > 2000

    def test_every_code_resolves_to_a_valid_internal_key(self) -> None:
        bad = []
        for c in naics_lookup._CODES:
            key = naics_lookup.resolve_internal_key(c["code"])
            if key not in INDUSTRY_LABEL_MAP:
                bad.append((c["code"], key))
        assert not bad, f"{len(bad)} NAICS codes resolved to an unmapped key: {bad[:10]}"

    def test_default_internal_key_is_itself_valid(self) -> None:
        assert naics_lookup._DEFAULT_INTERNAL_KEY in INDUSTRY_LABEL_MAP

    def test_ranged_sector_code_resolves(self) -> None:
        # "31-33" (Manufacturing) must strip to "31" before prefix matching.
        rec = naics_lookup.naics_lookup("31-33")
        assert rec is not None
        assert rec["internal_key"] in INDUSTRY_LABEL_MAP

    def test_unknown_code_falls_back_to_default(self) -> None:
        assert (
            naics_lookup.resolve_internal_key("999999")
            == naics_lookup._DEFAULT_INTERNAL_KEY
        )

    def test_empty_code_falls_back_to_default(self) -> None:
        assert (
            naics_lookup.resolve_internal_key("") == naics_lookup._DEFAULT_INTERNAL_KEY
        )
        assert (
            naics_lookup.resolve_internal_key(None)
            == naics_lookup._DEFAULT_INTERNAL_KEY
        )


# ═══════════════════════════════════════════════════════════════════════
# 2. naics_search() ranking
# ═══════════════════════════════════════════════════════════════════════


class TestNaicsSearch:
    def test_numeric_exact_code_match(self) -> None:
        results = naics_lookup.naics_search("621330")
        assert results
        assert results[0]["code"] == "621330"
        assert results[0]["internal_key"] == "mental_health"

    def test_numeric_prefix_match(self) -> None:
        results = naics_lookup.naics_search("5411")
        assert results
        codes = [r["code"] for r in results]
        assert any(c.startswith("5411") for c in codes)

    def test_multi_token_text_match_all_tokens_required(self) -> None:
        results = naics_lookup.naics_search("law office")
        assert results
        for r in results:
            title_lower = r["title"].lower()
            assert "law" in title_lower or "lawyer" in title_lower
            assert "office" in title_lower

    def test_text_match_returns_internal_key_and_level(self) -> None:
        results = naics_lookup.naics_search("mental health")
        assert results
        for r in results:
            assert set(r.keys()) == {"code", "title", "level", "internal_key"}
            assert r["internal_key"] in INDUSTRY_LABEL_MAP

    def test_no_match_returns_empty(self) -> None:
        assert naics_lookup.naics_search("zzznotarealindustryxyz") == []

    def test_empty_query_returns_empty(self) -> None:
        assert naics_lookup.naics_search("") == []
        assert naics_lookup.naics_search(None) == []

    def test_limit_is_respected(self) -> None:
        results = naics_lookup.naics_search("office", limit=3)
        assert len(results) <= 3

    def test_limit_default_caps_at_reasonable_size(self) -> None:
        results = naics_lookup.naics_search("services", limit=500)
        # search() itself clamps an oversized limit request
        assert len(results) <= 50

    def test_exact_code_ranks_above_prefix(self) -> None:
        # "5411" should surface the exact-length code before longer
        # prefix-only matches when both are present.
        results = naics_lookup.naics_search("54")
        assert results[0]["code"] == "54"  # exact sector code, top rank

    # ── Design panel 2026-07-31, iteration 2 (mechanism, accepted defect) ──
    # A parent code that aggregates a single child duplicates that child's
    # title verbatim (92213 "Legal Counsel and Prosecution" == 922130's
    # title; 54111 "Offices of Lawyers" == 541110's). Showing both rows in
    # a typeahead is pure noise -- only the deepest row should survive.

    def test_legal_counsel_query_has_no_duplicate_titles(self) -> None:
        results = naics_lookup.naics_search("legal counsel")
        titles = [r["title"] for r in results]
        assert len(titles) == len(set(titles)), f"duplicate titles: {titles}"

    def test_law_office_query_has_no_duplicate_titles(self) -> None:
        results = naics_lookup.naics_search("law office")
        titles = [r["title"] for r in results]
        assert len(titles) == len(set(titles)), f"duplicate titles: {titles}"

    def test_dedupe_keeps_the_deepest_row(self) -> None:
        # 922130 (level 6) and 92213 (level 5) share a title; the search
        # result for the pair must resolve to the 6-digit code.
        results = naics_lookup.naics_search("legal counsel")
        matches = [r for r in results if r["title"] == "Legal Counsel and Prosecution"]
        assert len(matches) == 1
        assert matches[0]["code"] == "922130"
        assert matches[0]["level"] == 6

        results2 = naics_lookup.naics_search("law office")
        matches2 = [r for r in results2 if r["title"] == "Offices of Lawyers"]
        assert len(matches2) == 1
        assert matches2[0]["code"] == "541110"
        assert matches2[0]["level"] == 6

    def test_dedupe_does_not_remove_genuinely_distinct_titles(self) -> None:
        # Sanity check the dedupe is title-scoped, not a blunt cap --
        # a broad query should still return multiple distinct titles.
        results = naics_lookup.naics_search("services", limit=20)
        titles = {r["title"] for r in results}
        assert len(titles) > 1


# ═══════════════════════════════════════════════════════════════════════
# 2b. Mapping fix: 922130/92213 "Legal Counsel and Prosecution" (design
# panel 2026-07-31, iteration 2, accepted defect -- used to fall through
# the "92" catch-all to general_entry_level instead of legal_services)
# ═══════════════════════════════════════════════════════════════════════


class TestLegalCounselMapping:
    def test_922130_resolves_to_legal_services(self) -> None:
        assert naics_lookup.resolve_internal_key("922130") == "legal_services"

    def test_92213_resolves_to_legal_services(self) -> None:
        assert naics_lookup.resolve_internal_key("92213") == "legal_services"

    def test_92211_courts_resolves_to_legal_services(self) -> None:
        assert naics_lookup.resolve_internal_key("92211") == "legal_services"

    def test_unrelated_92_sibling_still_falls_back_to_default(self) -> None:
        # Only 92211/92213 were added -- a sibling under "922" with no
        # specific override (e.g. a made-up/unmapped code under the
        # "92" public-administration catch-all) must still fall back to
        # default_internal_key, proving the fix is scoped, not a blanket
        # "92" -> legal_services change.
        assert (
            naics_lookup.resolve_internal_key("9229")
            == naics_lookup._DEFAULT_INTERNAL_KEY
        )


# ═══════════════════════════════════════════════════════════════════════
# 3. format_industry_label()
# ═══════════════════════════════════════════════════════════════════════


class TestFormatIndustryLabel:
    def test_no_naics_returns_label_unchanged(self) -> None:
        assert format_industry_label("Legal Services") == "Legal Services"
        assert format_industry_label("Legal Services", "", "") == "Legal Services"
        assert format_industry_label("Legal Services", None, None) == "Legal Services"

    def test_naics_code_and_title_appended(self) -> None:
        out = format_industry_label("Legal Services", "5411", "Law Offices")
        assert out == "Legal Services · NAICS 5411 — Law Offices"

    def test_naics_code_only_no_title(self) -> None:
        out = format_industry_label("Legal Services", "5411", "")
        assert out == "Legal Services · NAICS 5411"

    def test_empty_label_with_naics_still_produces_suffix(self) -> None:
        out = format_industry_label("", "5411", "Law Offices")
        assert out == "NAICS 5411 — Law Offices"

    def test_whitespace_only_naics_code_is_treated_as_absent(self) -> None:
        assert format_industry_label("Legal Services", "   ", "Title") == "Legal Services"


# ═══════════════════════════════════════════════════════════════════════
# 4. /api/naics/search live HTTP smoke test
# ═══════════════════════════════════════════════════════════════════════


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server() -> Iterator[int]:
    port = _free_port()
    server = app_module.ThreadedHTTPServer(
        ("127.0.0.1", port), app_module.MediaPlanHandler
    )
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="test-naics-http-server"
    )
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("live_server did not start accepting connections in time")
    yield port
    server.shutdown()
    server.server_close()


def _http_get(port: int, path: str, timeout: float = 5.0):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read()
        return resp.status, body
    finally:
        conn.close()


class TestNaicsSearchEndpointLive:
    def test_missing_q_returns_400(self, live_server: int) -> None:
        status, body = _http_get(live_server, "/api/naics/search")
        assert status == 400
        data = json.loads(body)
        assert "error" in data

    def test_valid_query_returns_200_with_expected_shape(self, live_server: int) -> None:
        status, body = _http_get(live_server, "/api/naics/search?q=621330")
        assert status == 200
        data = json.loads(body)
        assert "results" in data
        assert isinstance(data["results"], list)
        assert data["results"], "expected at least one match for an exact NAICS code"
        first = data["results"][0]
        assert set(first.keys()) == {
            "code",
            "title",
            "level",
            "internal_key",
            "internal_label",
        }
        assert first["internal_label"] == INDUSTRY_LABEL_MAP.get(
            first["internal_key"]
        )

    def test_no_match_returns_empty_results_not_error(self, live_server: int) -> None:
        status, body = _http_get(
            live_server, "/api/naics/search?q=zzzznotarealindustryxyz"
        )
        assert status == 200
        data = json.loads(body)
        assert data["results"] == []

    def test_limit_param_respected(self, live_server: int) -> None:
        status, body = _http_get(
            live_server, "/api/naics/search?q=services&limit=2"
        )
        assert status == 200
        data = json.loads(body)
        assert len(data["results"]) <= 2


# ═══════════════════════════════════════════════════════════════════════
# 5. Plan-payload round-trip
# ═══════════════════════════════════════════════════════════════════════


class TestPlanPayloadRoundTrip:
    def test_round_trip_with_naics_fields(self) -> None:
        payload = {
            "client_name": "Acme Legal",
            "industry": "legal_services",
            "industry_label": "Legal Services · NAICS 5411 — Law Offices",
            "naics_selected_code": "5411",
            "naics_selected_title": "Law Offices",
            "budget": "$50,000",
            "roles": ["Paralegal"],
            "locations": ["Chicago, IL"],
        }
        plan = PlanData.from_dict(payload)
        out = plan.to_dict()
        assert out["naics_selected_code"] == "5411"
        assert out["naics_selected_title"] == "Law Offices"
        assert out["industry_label"] == "Legal Services · NAICS 5411 — Law Offices"
        assert out["industry"] == "legal_services"

    def test_round_trip_without_naics_fields_unchanged(self) -> None:
        """Backward compatibility: a plan with no naics fields must
        round-trip identically to today (no naics keys invented)."""
        payload = {
            "client_name": "Acme Corp",
            "industry": "tech_engineering",
            "industry_label": "Technology & Engineering",
            "budget": "$50,000",
            "roles": ["Software Engineer"],
            "locations": ["Austin, TX"],
        }
        plan = PlanData.from_dict(payload)
        out = plan.to_dict()
        assert "naics_selected_code" not in out
        assert "naics_selected_title" not in out
        assert out["industry_label"] == "Technology & Engineering"

    def test_missing_naics_fields_do_not_block_construction(self) -> None:
        """A minimal/legacy payload with no naics fields at all must still
        construct a valid PlanData (never raise)."""
        plan = PlanData.from_dict({"industry": "general_entry_level"})
        assert plan.industry == "general_entry_level"
