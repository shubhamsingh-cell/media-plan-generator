"""Behaviour tests for the seven sites the WIDENED precedence lint found
(``and``-chain earlier operands and ``or ""`` string defaults).

Same bug class as test_precedence_site_fixes.py: a default literal attached to
the wrong expression, so ``X or "" == Y`` parses as ``X or ("" == Y)`` and
``A and B or 0 > 20`` parses as ``(A and B) or (0 > 20)``. Each class states
what the pre-fix code actually did and drives the REAL function where that is
cheap. The one exception is the async-generation location label (app.py): it
lives in a closure nested inside the request handler's background job, so the
expression is covered through its extracted helper plus a source check.
"""

from __future__ import annotations

import io
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pytest

import api_enrichment
import app
import budget_engine
import hire_signal
import nova_slack

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestHireSignalFunnelApplicationsGate:
    """hire_signal.generate_recommendations, 'Improve ... Funnel Efficiency'.

    Pre-fix: ``app_to_hire > 0 and app_to_hire < 1.0 and total_applications or
    0 > 20`` parses as ``(A and B and total_applications) or (0 > 20)``. The
    ``> 20`` volume gate never ran: ANY non-zero application count (even 3)
    fired the recommendation for a source converting under 1%.
    """

    _UNSET = object()

    def _funnel_titles(self, app_to_hire: float, total_applications: Any) -> list[str]:
        src: dict[str, Any] = {
            "source": "Indeed",
            "avg_qoh_score": 60,
            "total_hires": 1,
            "qoh_grade": "C",
            "quality_hire_pct": 50.0,
            "conversion_rates": {"app_to_hire": app_to_hire},
        }
        if total_applications is not self._UNSET:
            src["total_applications"] = total_applications
        recs = hire_signal.generate_recommendations(
            {"source_effectiveness": {"sources": {"Indeed": src}}}
        )
        return [r["title"] for r in recs if r["category"] == "funnel"]

    def test_small_sample_does_not_fire(self) -> None:
        assert self._funnel_titles(0.5, 3) == []

    def test_boundary_of_twenty_applications_does_not_fire(self) -> None:
        assert self._funnel_titles(0.5, 20) == []

    def test_large_sample_fires(self) -> None:
        assert self._funnel_titles(0.5, 50) == ["Improve Indeed Funnel Efficiency"]

    @pytest.mark.parametrize("apps", [_UNSET, None, 0])
    def test_missing_or_zero_applications_does_not_fire(self, apps: Any) -> None:
        assert self._funnel_titles(0.5, apps) == []

    def test_healthy_conversion_does_not_fire_even_with_volume(self) -> None:
        assert self._funnel_titles(2.0, 500) == []


class TestChannelSwapRemovesMatchingCategory:
    """budget_engine.simulate_channel_swap, category-based match.

    Pre-fix: ``ch_data.get("category") or "" == remove_cat`` parses as
    ``category or ("" == remove_cat)`` -- true for ANY channel that has a
    category -- so asking to remove a programmatic channel removed whichever
    channel came first (here Indeed, $5,000) and redistributed the wrong budget.
    """

    @staticmethod
    def _base() -> dict[str, Any]:
        return {
            "channel_allocations": {
                "Indeed": {
                    "dollar_amount": 5000,
                    "category": "job_board",
                    "projected_hires": 10,
                    "projected_applications": 200,
                },
                "Meta Facebook": {
                    "dollar_amount": 3000,
                    "category": "social",
                    "projected_hires": 4,
                    "projected_applications": 90,
                },
                "Programmatic DSP": {
                    "dollar_amount": 2000,
                    "category": "programmatic",
                    "projected_hires": 3,
                    "projected_applications": 70,
                },
            },
            "metadata": {"collar_type_used": "white_collar", "industry": "technology"},
        }

    def test_category_match_removes_the_matching_channel(self) -> None:
        out = budget_engine.simulate_channel_swap(
            self._base(), remove_channel="Programmatic Display"
        )
        assert out["freed_budget"] == 2000
        remaining = out["impact"]["budget_redistribution"]
        assert set(remaining) == {"Indeed", "Meta Facebook"}

    def test_no_channel_in_that_category_reports_not_found(self) -> None:
        base = self._base()
        del base["channel_allocations"]["Programmatic DSP"]
        out = budget_engine.simulate_channel_swap(
            base, remove_channel="Programmatic Display"
        )
        assert out["freed_budget"] == 0.0  # nothing was removed
        assert any("not found" in r for r in out["recommendations"])

    def test_exact_name_match_is_unchanged(self) -> None:
        out = budget_engine.simulate_channel_swap(
            self._base(), remove_channel="meta facebook"
        )
        assert out["freed_budget"] == 3000


class TestLocationLabel:
    """app._location_label (async-generation location label).

    Pre-fix: ``(l.get("city") or "" + ", " + l.get("state") or "")`` parses as
    ``l.get("city") or ("" + ", " + l.get("state")) or ""``: a dict with a city
    became just ``'Denver'`` (the state was dropped, so the Supabase / vector
    lookups ran on a bare city), and a dict with no city but a None state
    raised ``TypeError``. The sync path already builds "City, ST".
    """

    def test_city_and_state(self) -> None:
        assert app._location_label({"city": "Denver", "state": "CO"}) == "Denver, CO"

    def test_city_only_has_no_trailing_separator(self) -> None:
        assert app._location_label({"city": "Denver"}) == "Denver"
        assert app._location_label({"city": "Denver", "state": None}) == "Denver"

    def test_state_only(self) -> None:
        assert app._location_label({"state": "CO"}) == "CO"

    def test_empty_and_none_fields_do_not_raise(self) -> None:
        assert app._location_label({"city": None, "state": None}) == ""
        assert app._location_label({}) == ""

    def test_string_location_passes_through(self) -> None:
        assert app._location_label("Denver, CO") == "Denver, CO"

    def test_async_path_uses_the_helper(self) -> None:
        """The closure cannot be driven without the whole async job, so pin
        that it calls the helper and no longer carries the broken expression."""
        src = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")
        assert src.count("_location_label(") == 1 + 1  # def + async call site
        assert 'l.get("city") or "" + ", "' not in src


class TestBlsQcewPrivateOwnershipRow:
    """api_enrichment.fetch_industry_employment, national-row filter.

    Pre-fix: ``area == "US000" and row.get("own_code") or "" == "5"`` parses as
    ``(area == "US000" and own_code) or ("" == "5")`` -- any national row with a
    non-empty ownership code matched, so the first row (own_code "0", ALL
    ownerships) was returned instead of the private-sector row the comment and
    the ``"5"`` literal call for.
    """

    HEADER = (
        "area_fips,own_code,annual_avg_emplvl,annual_avg_wkly_wage,annual_avg_estabs"
    )

    @pytest.fixture(autouse=True)
    def _isolate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
        monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, data: None)
        monkeypatch.setattr(api_enrichment, "get_naics_code", lambda industry: "62")

    def _serve(self, monkeypatch: pytest.MonkeyPatch, rows: list[str]) -> None:
        csv = "\n".join([self.HEADER, *rows])
        monkeypatch.setattr(
            api_enrichment, "_http_get_text", lambda url, timeout=10: csv
        )

    def test_uses_the_private_ownership_row(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._serve(
            monkeypatch,
            [
                "01000,5,50,700,1",  # not national
                "US000,0,1000,900,10",  # national, ALL ownerships (first)
                "US000,1,200,1100,2",  # national, federal
                "US000,5,800,850,8",  # national, private  <-- the one we want
            ],
        )
        out = api_enrichment.fetch_industry_employment("healthcare")
        assert out is not None
        assert out["total_employed"] == 800
        assert out["avg_weekly_wage"] == 850
        assert out["establishments"] == 8

    def test_no_private_national_row_yields_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._serve(monkeypatch, ["US000,0,1000,900,10", "US000,1,200,1100,2"])
        assert api_enrichment.fetch_industry_employment("healthcare") is None


class TestGeoNamesNearbyExcludesSelf:
    """api_enrichment.fetch_geonames_data, nearby-cities list.

    Pre-fix: ``n.get("name") or "" != entry["name"]`` parses as
    ``name or ("" != entry["name"])`` -- always true for a named place -- so the
    queried city itself was listed as one of its own "nearby cities".
    """

    def test_the_city_itself_is_not_listed_as_nearby(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GEONAMES_USERNAME", "unit_test_user")
        monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
        monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, data: None)

        def _fake_get_json(url: str, timeout: int = 8) -> dict[str, Any]:
            if "searchJSON" in url:
                return {
                    "geonames": [
                        {
                            "name": "Austin",
                            "lat": "30.27",
                            "lng": "-97.74",
                            "population": 900000,
                            "countryName": "United States",
                            "countryCode": "US",
                        }
                    ]
                }
            if "timezoneJSON" in url:
                return {
                    "timezoneId": "America/Chicago",
                    "gmtOffset": -6,
                    "dstOffset": -5,
                }
            return {
                "geonames": [
                    {"name": "Austin", "population": 900000, "distance": 0},
                    {"name": "Round Rock", "population": 120000, "distance": 27.5},
                    {"name": "Pflugerville", "population": 65000, "distance": 21.0},
                ]
            }

        monkeypatch.setattr(api_enrichment, "_http_get_json", _fake_get_json)
        out = api_enrichment.fetch_geonames_data(["Austin, TX"])
        nearby = out["locations"]["Austin, TX"]["nearby_cities"]
        assert [n["name"] for n in nearby] == ["Round Rock", "Pflugerville"]


class TestSlackWeeklyDigestWindow:
    """nova_slack.NovaSlackBot.generate_weekly_digest.

    Pre-fix: ``status == "pending" and ts or "" >= week_ago`` parses as
    ``(status == "pending" and ts) or ("" >= week_ago)``; the date comparison
    collapsed to ``"" >= week_ago`` (False), so the "this week" counts were
    simply every pending / answered question of all time.
    """

    @staticmethod
    def _bot(questions: list[dict[str, Any]]) -> nova_slack.NovaSlackBot:
        bot = nova_slack.NovaSlackBot.__new__(nova_slack.NovaSlackBot)
        bot.unanswered = {"questions": questions}
        bot.learned_answers = {"metadata": {"total_learned": 4}}
        return bot

    @staticmethod
    def _iso(days_ago: float) -> str:
        return (datetime.utcnow() - timedelta(days=days_ago)).isoformat()

    def test_counts_only_the_last_seven_days(self) -> None:
        bot = self._bot(
            [
                {
                    "question": "new pending",
                    "status": "pending",
                    "timestamp": self._iso(2),
                },
                {
                    "question": "old pending",
                    "status": "pending",
                    "timestamp": self._iso(30),
                },
                {"question": "no ts pending", "status": "pending"},
                {
                    "question": "new answered",
                    "status": "answered",
                    "answered_at": self._iso(1),
                },
                {
                    "question": "old answered",
                    "status": "answered",
                    "answered_at": self._iso(40),
                },
            ]
        )
        digest = bot.generate_weekly_digest()
        assert "*1* new questions this week" in digest
        assert "*1* questions answered this week" in digest
        # The all-time pending total still counts every pending question.
        assert "*3* questions still pending review" in digest
        # Only the genuinely new question is listed for attention.
        assert "new pending" in digest
        assert "old pending" not in digest


class _FakeHttpResponse(io.BytesIO):
    """Minimal urlopen() response: context manager with a bytes ``read``."""

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class TestBingEstimatesRealPath:
    """api_enrichment._bing_ads_soap_request end-to-end (urlopen mocked).

    Pre-fix: each of avg_cpc / impressions / clicks was ``min or (0 + max) or 0``
    then ``/ 2.0`` -- the Minimum alone, halved -- instead of the Min/Max mean.
    """

    def test_estimates_are_min_max_means(self, monkeypatch: pytest.MonkeyPatch) -> None:
        bodies = [
            {"KeywordIdeas": [{"Keyword": "nurse", "AvgMonthlySearches": 500}]},
            {
                "CampaignEstimates": [
                    {
                        "AdGroupEstimates": [
                            {
                                "KeywordEstimates": [
                                    {
                                        "Minimum": {
                                            "AverageCpc": 1.0,
                                            "Impressions": 100,
                                            "Clicks": 10,
                                        },
                                        "Maximum": {
                                            "AverageCpc": 3.0,
                                            "Impressions": 300,
                                            "Clicks": 30,
                                        },
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
        ]
        calls: list[Optional[str]] = []

        def _fake_urlopen(req: Any, *args: Any, **kwargs: Any) -> _FakeHttpResponse:
            calls.append(getattr(req, "full_url", None))
            return _FakeHttpResponse(json.dumps(bodies[len(calls) - 1]).encode("utf-8"))

        monkeypatch.setattr(api_enrichment.urllib.request, "urlopen", _fake_urlopen)
        out = api_enrichment._bing_ads_soap_request(
            "tok", "dev", "cust", "acct", ["nurse"]
        )
        assert len(calls) == 2
        assert out is not None
        est = out["nurse"]
        assert est["avg_cpc_usd"] == pytest.approx(2.0)
        assert est["estimated_daily_impressions"] == pytest.approx(200.0)
        assert est["estimated_daily_clicks"] == pytest.approx(20.0)
        # cpm = cpc * clicks / impressions * 1000 on the MEANS: 2 * 20 / 200 * 1000
        assert est["avg_cpm_usd"] == pytest.approx(200.0)
