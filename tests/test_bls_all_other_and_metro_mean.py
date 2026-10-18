"""Two BLS wage defects found by the independent verifier (2026-10-18).

1. "All Other" catch-all occupations still got a live wage. ``program manager``
   maps to SOC 11-9199 (Managers, All Other); the same bucket was already
   refused for ``project manager`` alone. A residual bucket is whatever is left
   once the named occupations are taken out, so its national mean is no wage for
   one job title: Program Manager (Chicago) printed $156,090 as "BLS OES" (the
   all-managers mean x 1.10) over a better H-1B figure. Risk Manager (11-9199),
   Solutions Architect and SAP Consultant (15-1299), Machine Operator and
   Production Worker (51-9199) had the same problem. Every SOC ending ``99`` is
   an "All Other" bucket (all 46 in the May 2025 OEWS title list are).

2. The budget simulator's metro cost multiplier divided the all-occupation metro
   MEAN by a national MEDIAN-shaped constant (59,540), so an average metro read
   about 1.17x and New York capped at 1.40. It now divides by the national
   all-occupation MEAN of the same vintage (series OEUN000000000000000000004,
   May 2025: 69,770), fetched in the same POST as the metro series.

BLS figures below are the May 2025 values read from the live BLS public API on
2026-10-18 (all-occupation annual mean, datatype 04). No test touches the
network.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import bls_oews  # noqa: E402
import budget_simulator  # noqa: E402
import data_synthesizer  # noqa: E402
from kb_loader import load_knowledge_base  # noqa: E402

# ----------------------------------------------------- 1. All Other SOCs ---

ALL_OTHER_TITLES = [
    "Program Manager",  # 11-9199 Managers, All Other
    "Risk Manager",  # 11-9199
    "Solutions Architect",  # 15-1299 Computer Occupations, All Other
    "SAP Consultant",  # 15-1299
    "Machine Operator",  # 51-9199 Production Workers, All Other
    "Production Worker",  # 51-9199
]
# control titles whose SOC is a real, named occupation (not ending 99)
CONTROL_TITLES = {
    "Registered Nurse": "29-1141",
    "Software Engineer": "15-1252",
    "Senior Software Engineer": "15-1252",  # longest-title-run path
}


@pytest.fixture()
def any_soc_bls(monkeypatch: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
    """BLS that answers EVERY well-formed national series with a wage, so a
    title that reaches the API gets a number (the pre-fix behaviour)."""
    posts: List[Dict[str, Any]] = []

    def post(url: str, payload: Dict[str, Any], timeout: int = 12) -> Dict[str, Any]:
        posts.append(dict(payload, url=url))
        series = []
        for sid in payload["seriesid"]:
            base = {"04": 153890, "13": 141900, "11": 90000, "15": 230000}[sid[-2:]]
            series.append(
                {
                    "seriesID": sid,
                    "data": [
                        {
                            "year": "2025",
                            "period": "A01",
                            "periodName": "Annual",
                            "value": str(base),
                        }
                    ],
                }
            )
        return {
            "status": "REQUEST_SUCCEEDED",
            "message": [],
            "Results": {"series": series},
        }

    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, value: None)
    monkeypatch.setattr(api_enrichment, "_rate_limit_acquire", lambda name: True)
    monkeypatch.setattr(api_enrichment, "_http_post_json", post)
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    api_enrichment._BLS_FAILURES.clear()
    return posts


@pytest.mark.parametrize(
    "soc,expected",
    [
        ("11-9199", True),  # Managers, All Other
        ("15-1299", True),  # Computer Occupations, All Other
        ("51-9199", True),  # Production Workers, All Other
        ("29-9099", True),  # Healthcare Practitioners and Technical, All Other
        ("15-1299.00", True),  # O*NET-SOC suffix
        ("151299", True),  # no hyphen
        ("29-1141", False),  # Registered Nurses
        ("15-1252", False),  # Software Developers
        ("11-9111", False),  # Medical and Health Services Managers
        ("11-9190", False),  # ends 90, not 99
        ("", False),
        (None, False),
        ("not a soc", False),
        ("15-12", False),
    ],
)
def test_all_other_is_a_soc_ending_99(soc: Any, expected: bool) -> None:
    assert bls_oews.is_all_other_soc(soc) is expected


def test_the_named_titles_really_sit_on_all_other_buckets() -> None:
    """Pins the premise: if SOC_CODES is ever corrected, this test says the
    refusal below no longer has anything to refuse for that title."""
    expected = {
        "program manager": "11-9199",
        "risk manager": "11-9199",
        "solutions architect": "15-1299",
        "sap consultant": "15-1299",
        "machine operator": "51-9199",
        "production worker": "51-9199",
    }
    assert {t: api_enrichment.SOC_CODES[t] for t in expected} == expected


@pytest.mark.parametrize("title", ALL_OTHER_TITLES)
def test_an_all_other_title_gets_no_bls_wage_and_costs_no_request(
    any_soc_bls: List[Dict[str, Any]], title: str
) -> None:
    assert api_enrichment.fetch_salary_data([title]) == {}
    assert any_soc_bls == []  # the API was never asked
    soc, why = api_enrichment._soc_for_salary_role(title)
    assert soc == "" and "All Other" in why and "benchmark kept" in why
    reasons = api_enrichment.salary_skip_reasons([title], {})
    assert "All Other" in reasons[title]


@pytest.mark.parametrize(
    "title",
    ["Senior Program Manager", "Program Manager II", "Sr Machine Operator"],
)
def test_a_seniority_variant_of_an_all_other_title_is_refused_too(
    any_soc_bls: List[Dict[str, Any]], title: str
) -> None:
    """The longest-title-run path reaches the same bucket."""
    assert api_enrichment.fetch_salary_data([title]) == {}
    assert any_soc_bls == []


def test_no_title_in_the_table_reaches_an_all_other_bucket() -> None:
    """The rule is on the SOC, so a title added to SOC_CODES later is covered
    without being listed in _BLS_NO_WAGE_TITLES."""
    on_bucket = [
        t
        for t, soc in api_enrichment.SOC_CODES.items()
        if bls_oews.is_all_other_soc(soc)
    ]
    assert {t.lower() for t in ALL_OTHER_TITLES} <= set(on_bucket)
    for title in on_bucket:
        assert api_enrichment._soc_for_salary_role(title)[0] == "", title


@pytest.mark.parametrize("title,soc", sorted(CONTROL_TITLES.items()))
def test_a_named_occupation_still_gets_its_wage(
    any_soc_bls: List[Dict[str, Any]], title: str, soc: str
) -> None:
    out = api_enrichment.fetch_salary_data([title])
    assert out[title]["soc_code"] == soc
    assert out[title]["source"] == "BLS OES" and out[title]["median"] == 141900
    assert len(any_soc_bls) == 1


@pytest.mark.parametrize("title", ALL_OTHER_TITLES)
def test_the_fused_salary_falls_back_instead_of_printing_a_bls_label(
    any_soc_bls: List[Dict[str, Any]], title: str
) -> None:
    """End to end through the fusion that builds the "Sources:" line: the
    result for an All Other title is exactly what it is with no BLS at all
    (H-1B, the curated industry benchmark, or "not available"), never
    "BLS OES"."""
    kb = load_knowledge_base()
    plan = {
        "roles": [title],
        "industry": "general",
        "locations": ["Chicago, IL"],
    }
    with_bls = data_synthesizer.fuse_salary_intelligence(
        {"salary_data": api_enrichment.fetch_salary_data([title])}, kb, dict(plan)
    )
    without = data_synthesizer.fuse_salary_intelligence({}, kb, dict(plan))
    assert "BLS OES" not in json.dumps(with_bls)
    assert with_bls == without


def test_the_fused_fallback_label_is_the_curated_or_h1b_one(
    any_soc_bls: List[Dict[str, Any]],
) -> None:
    kb = load_knowledge_base()

    def sources(title: str) -> Optional[List[str]]:
        plan = {"roles": [title], "industry": "general", "locations": ["Chicago, IL"]}
        fused = data_synthesizer.fuse_salary_intelligence(
            {"salary_data": api_enrichment.fetch_salary_data([title])}, kb, plan
        )
        return (fused.get(title) or {}).get("sources")

    assert sources("Program Manager") == ["DOL H-1B/LCA"]
    assert sources("SAP Consultant") == ["DOL H-1B/LCA"]
    assert sources("Risk Manager") == ["Industry Benchmark"]
    # a named occupation keeps BLS beside H-1B
    assert "BLS OES" in (sources("Registered Nurse") or [])


# ------------------------------------------- 2. the metro cost multiplier ---

# All-occupation annual MEAN wage (datatype 04), May 2025, live BLS 2026-10-18.
NATIONAL_MEAN_2025 = 69_770
METRO_MEAN_2025 = {
    "35620": 86_310,  # New York-Newark-Jersey City
    "19100": 70_630,  # Dallas-Fort Worth-Arlington
    "12420": 74_580,  # Austin-Round Rock-San Marcos
    "16980": 73_380,  # Chicago-Naperville-Elgin
}
NATIONAL_ID = "OEUN000000000000000000004"


def _bls_response(
    ids: List[str],
    *,
    national: Optional[int] = NATIONAL_MEAN_2025,
    national_year: str = "2025",
    metro_year: str = "2025",
    metro_scale: float = 1.0,
) -> Dict[str, Any]:
    series = []
    for sid in ids:
        if sid == NATIONAL_ID:
            value: Optional[int] = national
            year = national_year
        else:
            cbsa = sid[6:11]  # OEUM + "00" + 5-digit CBSA
            value = int(METRO_MEAN_2025[cbsa] * metro_scale)
            year = metro_year
        data = (
            [
                {
                    "year": year,
                    "period": "A01",
                    "periodName": "Annual",
                    "value": str(value),
                }
            ]
            if value is not None
            else []
        )
        series.append({"seriesID": sid, "data": data})
    return {"status": "REQUEST_SUCCEEDED", "Results": {"series": series}}


@pytest.fixture()
def metro_bls(monkeypatch: pytest.MonkeyPatch):
    """fetch_metro_salary_data wired to a fake urlopen. Returns (sent, set_body):
    ``set_body(fn)`` swaps the response builder ``fn(ids) -> dict``."""
    sent: List[Dict[str, Any]] = []
    state: Dict[str, Any] = {"body": _bls_response, "raise": None}

    class _Resp:
        def __init__(self, body: Dict[str, Any]) -> None:
            self._body = body

        def __enter__(self) -> "_Resp":
            return self

        def __exit__(self, *exc: Any) -> bool:
            return False

        def read(self) -> bytes:
            return json.dumps(self._body).encode()

    def fake_urlopen(req: Any, timeout: Any = None) -> _Resp:
        if state["raise"]:
            raise state["raise"]
        payload = json.loads(req.data)
        sent.append(payload)
        return _Resp(state["body"](payload["seriesid"]))

    monkeypatch.setenv("BLS_API_KEY", "test-key")
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, value: None)
    monkeypatch.setattr(api_enrichment.urllib.request, "urlopen", fake_urlopen)
    return sent, state


def test_the_constant_is_a_dated_mean_not_a_median() -> None:
    assert not hasattr(budget_simulator, "_NATIONAL_MEDIAN_WAGE")
    assert budget_simulator._NATIONAL_MEAN_WAGE_MAY_2025 == float(NATIONAL_MEAN_2025)


def test_one_post_carries_the_metro_and_the_national_mean(metro_bls) -> None:
    sent, _state = metro_bls
    out = api_enrichment.fetch_metro_salary_data("Dallas")
    assert len(sent) == 1  # one request, not two
    assert sent[0]["seriesid"] == ["OEUM001910000000000000004", NATIONAL_ID]
    assert all(len(s) == 25 for s in sent[0]["seriesid"])
    assert out["annual_mean_wage"] == 70_630.0
    assert out["national_annual_mean_wage"] == float(NATIONAL_MEAN_2025)
    assert out["national_reference_period"] == out["reference_period"] == "May 2025"


def test_an_occupation_specific_metro_lookup_does_not_ask_for_the_national_mean(
    metro_bls,
) -> None:
    """The all-occupation national mean is no comparator for a one-occupation
    metro wage, so that call stays a single series."""
    sent, state = metro_bls
    state["body"] = lambda ids: {
        "Results": {
            "series": [
                {
                    "seriesID": ids[0],
                    "data": [{"year": "2025", "period": "A01", "value": "101420"}],
                }
            ]
        }
    }
    out = api_enrichment.fetch_metro_salary_data("Dallas", "29-1141")
    assert len(sent[0]["seriesid"]) == 1 and sent[0]["seriesid"][0].endswith("29114104")
    assert "national_annual_mean_wage" not in out


@pytest.mark.parametrize(
    "city,true_ratio",
    [
        ("Dallas", 1.01),  # 70,630 / 69,770
        ("Chicago", 1.05),  # 73,380 / 69,770
        ("Austin", 1.07),  # 74,580 / 69,770
        ("New York", 1.24),  # 86,310 / 69,770 -- under the 1.4 cap
    ],
)
def test_the_metro_multiplier_is_mean_over_same_vintage_mean(
    metro_bls, city: str, true_ratio: float
) -> None:
    got = budget_simulator._get_metro_salary_multiplier(city)
    assert got == pytest.approx(true_ratio, abs=0.05), (city, got)
    # before: / 59,540 -> Dallas 1.19, Chicago 1.23, Austin 1.25, New York capped
    # at 1.40; all four are outside the band above


def test_a_metro_exactly_at_the_national_mean_is_exactly_one(metro_bls) -> None:
    sent, state = metro_bls
    state["body"] = lambda ids: {
        "Results": {
            "series": [
                {
                    "seriesID": sid,
                    "data": [
                        {
                            "year": "2025",
                            "period": "A01",
                            "value": str(NATIONAL_MEAN_2025),
                        }
                    ],
                }
                for sid in ids
            ]
        }
    }
    assert budget_simulator._get_metro_salary_multiplier("Dallas") == 1.0


def test_no_national_in_the_answer_degrades_to_the_dated_constant(metro_bls) -> None:
    """A failed national series is neither 1.0 nor the old biased ratio: the
    May 2025 mean stands in."""
    _sent, state = metro_bls
    state["body"] = lambda ids: _bls_response(ids, national=None)
    out = api_enrichment.fetch_metro_salary_data("Dallas")
    assert "national_annual_mean_wage" not in out
    got = budget_simulator._get_metro_salary_multiplier("Dallas")
    assert got == pytest.approx(70_630 / NATIONAL_MEAN_2025, abs=0.001)
    assert abs(got - 70_630 / 59_540) > 0.1  # not the old ratio


def test_a_national_mean_of_another_vintage_is_not_mixed_in(metro_bls) -> None:
    _sent, state = metro_bls
    state["body"] = lambda ids: _bls_response(
        ids, national=64_000, national_year="2024", metro_year="2025"
    )
    out = api_enrichment.fetch_metro_salary_data("Dallas")
    assert out["year"] == "2025" and "national_annual_mean_wage" not in out
    got = budget_simulator._get_metro_salary_multiplier("Dallas")
    assert got == pytest.approx(70_630 / NATIONAL_MEAN_2025, abs=0.001)


def test_a_live_national_mean_is_preferred_over_the_constant(metro_bls) -> None:
    """When the next release lands the live pair is used, not the stale
    constant: a 3% higher national mean lowers the same metro's ratio."""
    _sent, state = metro_bls
    state["body"] = lambda ids: _bls_response(
        ids, national=int(NATIONAL_MEAN_2025 * 1.03), metro_scale=1.0
    )
    got = budget_simulator._get_metro_salary_multiplier("Austin")
    assert got == pytest.approx(74_580 / (NATIONAL_MEAN_2025 * 1.03), abs=0.001)


def test_the_clamp_is_kept(metro_bls) -> None:
    _sent, state = metro_bls
    state["body"] = lambda ids: _bls_response(ids, metro_scale=3.0)
    assert budget_simulator._get_metro_salary_multiplier("Dallas") == 1.4
    state["body"] = lambda ids: _bls_response(ids, metro_scale=0.1)
    assert budget_simulator._get_metro_salary_multiplier("Dallas") == 0.8


def test_an_empty_or_failed_bls_response_is_a_multiplier_of_one(
    metro_bls, monkeypatch: pytest.MonkeyPatch
) -> None:
    _sent, state = metro_bls
    state["body"] = lambda ids: {"Results": {"series": []}}
    assert budget_simulator._get_metro_salary_multiplier("Dallas") == 1.0
    state["body"] = lambda ids: {
        "Results": {"series": [{"seriesID": i, "data": []} for i in ids]}
    }
    assert budget_simulator._get_metro_salary_multiplier("Dallas") == 1.0
    state["raise"] = api_enrichment.urllib.error.URLError("unreachable")
    assert budget_simulator._get_metro_salary_multiplier("Dallas") == 1.0
    state["raise"] = None
    monkeypatch.delenv("BLS_API_KEY")
    assert budget_simulator._get_metro_salary_multiplier("Dallas") == 1.0
    monkeypatch.setenv("BLS_API_KEY", "test-key")
    assert budget_simulator._get_metro_salary_multiplier("Nowhereville") == 1.0
