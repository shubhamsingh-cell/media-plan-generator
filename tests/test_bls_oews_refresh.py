"""The BLS per-role wage refresh refreshed nothing (2026-10-18).

Prod logged "Enriched BLS salary data: 0/10 roles refreshed" on 13 of 13 runs
although BLS answered REQUEST_SUCCEEDED. Verified against the live BLS public
API: the national OEWS series IDs the code built were valid, but OEWS keeps ONE
vintage per series (reference year 2025, period A01) and every other year
answers "No Data Available" -- the code asked for 2022-2024. The metro builder
(23 chars) and ``BLSClient.get_occupational_employment`` (27 chars) never built
a valid ID at all.

Wage values below are the national OEWS May 2025 figures read from the live API
on 2026-10-18. No test touches the network: BLS transport is a fake that
reproduces the verified one-vintage behaviour.
"""

from __future__ import annotations

import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import bls_oews  # noqa: E402
import data_enrichment as de  # noqa: E402
import data_refresh  # noqa: E402
from tests.bls_wage_fixture import wage  # noqa: E402

# SOC -> (mean, median, p10, p90), national, May 2025 (live API 2026-10-18)
LIVE_2025: Dict[str, tuple] = {
    "15-1252": (148100, 135980, 82460, 214670),  # Software Developers
    "29-1141": (101420, 97550, 68940, 137470),  # Registered Nurses
    "11-2021": (177770, 166790, 90260, 293610),  # Marketing Managers
    "41-4012": (82530, 72080, 39090, 137550),  # Sales Reps, Wholesale/Mfg, Non-Tech
    "53-3032": (59710, 58640, 40140, 79380),  # Heavy and Tractor-Trailer Truck Drivers
    "53-7065": (39540, 37330, 30110, 49260),  # Stockers and Order Fillers
    # Laborers and Freight, Stock, and Material Movers, Hand: FIXTURE values for
    # the mocked API only (not a published BLS figure); O*NET lists Warehouse
    # Associate under 53-7062.00.
    "53-7062": (38000, 36000, 29000, 48000),
    "43-4051": (46590, 44770, 31750, 63590),  # Customer Service Representatives
    "13-2011": (94750, 83680, 56020, 144090),  # Accountants and Auditors
    "13-2051": (116800, 102740, None, None),  # Financial and Investment Analysts
    "11-9199": (153890, 141900, None, None),  # Managers, All Other
}
_FIELD_INDEX = {"mean": 0, "median": 1, "p10": 2, "p90": 3}


def _vintage() -> int:
    """The newest reference year BLS holds: always inside the current window."""
    return datetime.datetime.now().year - 1


def _fake_bls(vintage: int, calls: List[Dict[str, Any]]):
    """BLS API v1/v2 as verified live: REQUEST_SUCCEEDED always, observations
    only for the ONE published vintage and only if the request window contains
    it, otherwise "No Data Available"."""

    def post(url: str, payload: Dict[str, Any], timeout: int = 12) -> Dict[str, Any]:
        calls.append(dict(payload, url=url))
        start, end = int(payload["startyear"]), int(payload["endyear"])
        series: List[Dict[str, Any]] = []
        messages: List[str] = []
        for sid in payload["seriesid"]:
            assert len(sid) == 25 and sid.startswith("OE"), sid  # else: BLS says no
            soc = f"{sid[-8:-6]}-{sid[-6:-2]}"
            datatype = sid[-2:]
            field = {"04": "mean", "13": "median", "11": "p10", "15": "p90"}[datatype]
            value = LIVE_2025.get(soc, (None,) * 4)[_FIELD_INDEX[field]]
            data: List[Dict[str, Any]] = []
            if value is not None and start <= vintage <= end:
                data = [
                    {
                        "year": str(vintage),
                        "period": "A01",
                        "periodName": "Annual",
                        "value": str(value),
                        "footnotes": [{}],
                    }
                ]
            else:
                messages.append(f"No Data Available for Series {sid} Year: {start}")
            series.append({"seriesID": sid, "data": data})
        return {
            "status": "REQUEST_SUCCEEDED",
            "responseTime": 12,
            "message": messages,
            "Results": {"series": series},
        }

    return post


@pytest.fixture()
def bls(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    """api_enrichment wired to the fake BLS: no cache, no quota, no key."""
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, value: None)
    monkeypatch.setattr(api_enrichment, "_rate_limit_acquire", lambda name: True)
    monkeypatch.setattr(api_enrichment, "_http_post_json", _fake_bls(_vintage(), calls))
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    getattr(api_enrichment, "_BLS_FAILURES", {}).clear()
    return {"calls": calls, "mp": monkeypatch}


# ------------------------------------------------------- the ID builder ---


@pytest.mark.parametrize("soc", sorted(LIVE_2025))
@pytest.mark.parametrize("datatype", ["04", "11", "13", "15"])
def test_national_series_ids_are_25_characters_with_the_oeun_prefix(
    soc: str, datatype: str
) -> None:
    sid = bls_oews.oews_series_id(soc, datatype)
    assert len(sid) == 25 and sid.startswith("OEUN"), sid
    assert sid == "OEUN0000000" + "000000" + soc.replace("-", "") + datatype


def test_a_known_live_series_id_is_reproduced_exactly() -> None:
    # exists=true on the live API (Software Developers, annual median, national)
    assert bls_oews.oews_series_id("15-1252", "13") == "OEUN000000000000015125213"
    # the 23-character metro ID the old builder made: "Series does not exist"
    assert (
        bls_oews.oews_series_id(
            "15-1252",
            "13",
            areatype="M",
            area=bls_oews.cbsa_area_code("35620"),
        )
        == "OEUM003562000000015125213"
    )
    assert (
        bls_oews.oews_series_id(None, "04", areatype="M", area="0035620")
        == "OEUM003562000000000000004"
    )


@pytest.mark.parametrize(
    "soc", ["", "15-12", "15125200", "15-1252.0", "x", "15-12521", "1-1252", None]
)
def test_a_value_that_is_not_a_soc_never_becomes_a_series_id(soc: Any) -> None:
    if soc in (None, ""):
        # "all occupations" is the only thing an empty SOC means
        assert bls_oews.oews_series_id(soc, "04").endswith("00000004")
        return
    with pytest.raises(ValueError):
        bls_oews.oews_series_id(soc, "13")


def test_an_onet_soc_suffix_is_stripped_not_concatenated() -> None:
    # "15-1252.00".replace("-", "").replace(".", "") was "15125200" (8 digits)
    assert bls_oews.normalize_soc("15-1252.00") == "15-1252"
    assert bls_oews.oews_series_id("15-1252.00", "13") == (
        bls_oews.oews_series_id("151252", "13")
    )


def test_the_blsclient_per_role_path_builds_valid_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import api_integrations

    seen: List[List[str]] = []
    client = api_integrations.BLSClient()
    monkeypatch.setattr(
        client,
        "_fetch_series",
        lambda ids, *a, **k: seen.append(list(ids)) or {"ok": 1},
    )
    for soc in ("15-1252", "151252", "15-1252.00"):
        assert client.get_occupational_employment(soc) == {"ok": 1}
    assert len(seen) == 3 and seen[0] == seen[1] == seen[2]
    assert [len(i) for i in seen[0]] == [25, 25, 25]
    assert all(i.startswith("OEUN") for i in seen[0])
    assert [i[-2:] for i in seen[0]] == ["01", "04", "13"]
    # a SOC it cannot read is refused, not queried
    assert client.get_occupational_employment("15125200") is None
    assert len(seen) == 3


def test_the_metro_lookup_sends_a_valid_25_character_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: List[Dict[str, Any]] = []

    class _Resp:
        def __enter__(self) -> "_Resp":
            return self

        def __exit__(self, *exc: Any) -> bool:
            return False

        def read(self) -> bytes:
            return json.dumps(
                {
                    "Results": {
                        "series": [
                            {
                                "seriesID": sent[-1]["seriesid"][0],
                                "data": [
                                    {
                                        "year": "2025",
                                        "period": "A01",
                                        "periodName": "Annual",
                                        "value": "86310",
                                    }
                                ],
                            }
                        ]
                    }
                }
            ).encode()

    def fake_urlopen(req: Any, timeout: Any = None) -> _Resp:
        sent.append(json.loads(req.data))
        return _Resp()

    monkeypatch.setenv("BLS_API_KEY", "test-key")
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, value: None)
    monkeypatch.setattr(api_enrichment.urllib.request, "urlopen", fake_urlopen)
    out = api_enrichment.fetch_metro_salary_data("New York")
    ids = sent[0]["seriesid"]
    # the metro mean, then the national all-occupation mean of the same
    # vintage (OEUN + 0000000 + 000000 + 000000 + 04) in the same POST
    assert ids == ["OEUM003562000000000000004", "OEUN000000000000000000004"]
    assert [len(i) for i in ids] == [25, 25] and len(sent) == 1
    assert out["annual_mean_wage"] == 86310.0 and out["reference_period"] == "May 2025"
    assert int(sent[0]["endyear"]) == datetime.datetime.now().year


# ----------------------------------------------------------- the window ---


def test_the_request_window_ends_at_the_current_year() -> None:
    assert bls_oews.year_window(2026) == ("2023", "2026")
    assert bls_oews.year_window(2027) == ("2024", "2027")
    for this_year in (2026, 2027, 2030):  # the one published vintage stays inside
        start, end = map(int, bls_oews.year_window(this_year))
        assert start <= this_year - 1 <= end


def test_root_cause_a_window_that_misses_the_vintage_returns_nothing(
    bls: Dict[str, Any],
) -> None:
    """Before the fix: startyear 2022, endyear 2024 -> REQUEST_SUCCEEDED with
    empty series -> no wage, for every role."""
    fake = _fake_bls(_vintage(), [])
    old = fake(
        "u",
        {
            "seriesid": list(bls_oews.wage_series_ids("15-1252").values()),
            "startyear": "2022",
            "endyear": "2024",
        },
    )
    assert old["status"] == "REQUEST_SUCCEEDED"
    assert all(not s["data"] for s in old["Results"]["series"])
    assert bls_oews.build_wage_result("15-1252", old["Results"]["series"])[0] is None


def test_fetch_salary_data_returns_the_published_vintage(bls: Dict[str, Any]) -> None:
    out = api_enrichment.fetch_salary_data(["Software Engineer"])
    got = out["Software Engineer"]
    assert (got["mean"], got["median"], got["p10"], got["p90"]) == LIVE_2025["15-1252"]
    assert got["soc_code"] == "15-1252"
    assert got["reference_year"] == _vintage()
    assert got["reference_period"] == f"May {_vintage()}"
    assert got["series_ids"] == bls_oews.wage_series_ids("15-1252")
    sent = bls["calls"][0]
    assert sent["seriesid"] == list(bls_oews.wage_series_ids("15-1252").values())
    assert int(sent["endyear"]) == datetime.datetime.now().year


# --------------------------------------------- the 10 roles, end to end ---

_TOP_ROLES = de._TOP_SALARY_ROLES


def test_the_ten_refresh_roles_resolve_to_the_right_occupations(
    bls: Dict[str, Any],
) -> None:
    expected_soc = {
        "Software Engineer": "15-1252",
        "Registered Nurse": "29-1141",
        "Marketing Manager": "11-2021",
        "Sales Representative": "41-4012",
        "Truck Driver": "53-3032",
        # O*NET lists Warehouse Associate under 53-7062.00 (Laborers and Freight,
        # Stock, and Material Movers, Hand); 53-7065 is order filler / stocker.
        "Warehouse Associate": "53-7062",
        "Customer Service Rep": "43-4051",
        "Accountant": "13-2011",
    }
    out = api_enrichment.fetch_salary_data(_TOP_ROLES)
    assert {r: v["soc_code"] for r, v in out.items()} == expected_soc
    # no neighbour occupation's wage: Product Manager (SOC 2018 has none; the
    # table said Marketing Managers) and Data Analyst (said Data Scientists)
    skipped = api_enrichment.salary_skip_reasons(_TOP_ROLES, out)
    assert set(skipped) == {"Data Analyst", "Product Manager"}
    assert all("no BLS occupation is this job" in why for why in skipped.values())
    assert "11-2021" not in json.dumps(skipped)


def test_project_manager_is_not_the_managers_all_other_bucket(
    bls: Dict[str, Any],
) -> None:
    assert (
        api_enrichment.fetch_salary_data(["Project Manager", "Product Manager"]) == {}
    )
    assert bls["calls"] == []  # no request is spent on a role with no occupation


def test_refresh_reports_how_many_roles_and_why_the_rest_were_skipped(
    bls: Dict[str, Any],
) -> None:
    out = api_enrichment.fetch_salary_data(["Registered Nurse", "Data Analyst", "Chef"])
    reasons = api_enrichment.salary_skip_reasons(
        ["Registered Nurse", "Data Analyst", "Chef"], out
    )
    assert list(out) == ["Registered Nurse"]
    assert reasons["Data Analyst"].startswith("no BLS occupation is this job")
    assert reasons["Chef"].startswith("SOC 35-1011")


def test_unavailable_series_keep_the_old_value_and_say_why(
    bls: Dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    """BLS answers REQUEST_SUCCEEDED with empty series (the verified failure):
    nothing is returned, the reason names BLS's own message."""
    bls["mp"].setattr(
        api_enrichment, "_http_post_json", _fake_bls(2019, bls["calls"])
    )  # a vintage outside every window
    out = api_enrichment.fetch_salary_data(["Registered Nurse"])
    assert out == {}
    why = api_enrichment.salary_skip_reasons(["Registered Nurse"], out)
    assert why["Registered Nurse"].startswith("SOC 29-1141: bls_no_data")
    assert "No Data Available" in why["Registered Nurse"]


def test_data_refresh_pipeline_end_to_end_with_a_fake_bls(
    bls: Dict[str, Any], tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    bls["mp"].setattr(data_refresh, "_DATA_DIR", tmp_path)
    bls["mp"].setattr(
        data_refresh.DataRefreshPipeline,
        "_get_enrichment_module",
        staticmethod(lambda: api_enrichment),
    )
    pipe = data_refresh.DataRefreshPipeline()
    with caplog.at_level(logging.INFO, logger="data_refresh"):
        pipe._refresh_bls_salary()
    saved = json.loads((tmp_path / "bls_salary.json").read_text())["data"]
    assert set(saved) == {
        "Software Engineer",
        "Registered Nurse",
        "Marketing Manager",
        "Sales Representative",
        "Warehouse Associate",
        "Truck Driver",
        "Financial Analyst",
        "Customer Service Representative",
    }
    nurse = saved["Registered Nurse"]
    assert nurse["median"] == LIVE_2025["29-1141"][1]
    assert nurse["reference_period"] == f"May {_vintage()}"
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "refreshed: 8/10 roles" in text
    assert "Data Analyst (no BLS occupation is this job" in text
    assert "Project Manager (no BLS occupation is this job" in text
    assert pipe._last_refresh["bls_salary"]["success"] is True


def test_data_refresh_keeps_the_old_value_when_the_new_one_is_implausible(
    bls: Dict[str, Any], tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    bls["mp"].setattr(data_refresh, "_DATA_DIR", tmp_path)
    bls["mp"].setattr(
        data_refresh.DataRefreshPipeline,
        "_get_enrichment_module",
        staticmethod(lambda: api_enrichment),
    )
    previous_nurse = wage("29-1141", 40000, 30000, 60000)  # BLS says 97,550: +144%
    previous_engineer = wage("15-1252", 130000, 80000, 210000)  # within the band
    (tmp_path / "bls_salary.json").write_text(
        json.dumps(
            {
                "data": {
                    "Registered Nurse": previous_nurse,
                    "Software Engineer": previous_engineer,
                    "Chef": {"median": 51000},  # a role this run does not fetch
                }
            }
        )
    )
    pipe = data_refresh.DataRefreshPipeline()
    with caplog.at_level(logging.WARNING):
        pipe._refresh_bls_salary()
    saved = json.loads((tmp_path / "bls_salary.json").read_text())["data"]
    assert saved["Registered Nurse"]["median"] == 40000  # previous kept
    assert saved["Software Engineer"]["median"] == LIVE_2025["15-1252"][1]  # replaced
    assert saved["Chef"] == {"median": 51000}  # untouched
    warnings = " ".join(
        r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING
    )
    assert "Registered Nurse" in warnings and "outside +/-50%" in warnings
    assert "rejected, previous value kept" in warnings


def test_data_refresh_with_no_usable_wage_reports_zero_with_reasons(
    bls: Dict[str, Any], tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    bls["mp"].setattr(data_refresh, "_DATA_DIR", tmp_path)
    bls["mp"].setattr(
        data_refresh.DataRefreshPipeline,
        "_get_enrichment_module",
        staticmethod(lambda: api_enrichment),
    )
    bls["mp"].setattr(api_enrichment, "_http_post_json", _fake_bls(2019, []))
    pipe = data_refresh.DataRefreshPipeline()
    with caplog.at_level(logging.WARNING, logger="data_refresh"):
        pipe._refresh_bls_salary()
    assert not (tmp_path / "bls_salary.json").exists()
    status = pipe._last_refresh["bls_salary"]
    assert status["success"] is False
    assert status["error"].startswith("0 roles refreshed")
    assert "bls_no_data" in status["error"] and "Registered Nurse" in status["error"]
    assert any("refreshed 0/10 roles" in r.getMessage() for r in caplog.records)


def _supabase_capture(mp: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    mp.setattr(
        de, "_upsert_to_supabase", lambda table, r, **k: rows.extend(r) or len(r)
    )
    mp.setattr(de, "_generate_llm_summary", lambda *a, **k: "")
    return rows


def test_enrichment_engine_writes_provenance_and_skips_a_wild_jump(
    bls: Dict[str, Any], tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    mp = bls["mp"]
    rows = _supabase_capture(mp)
    marks: List[tuple] = []
    mp.setattr(de, "ENRICHMENT_STATE_FILE", tmp_path / "state.json")
    mp.setattr(de, "_SUPABASE_ENABLED", False)
    eng = de.DataEnrichmentEngine()
    mp.setattr(eng, "_is_stale", lambda source: True)
    mp.setattr(eng, "_mark_refreshed", lambda *a, **k: marks.append(a))
    mp.setattr(
        eng,
        "_load_previous_bls_wages",
        lambda: {
            "Registered Nurse": {"median_salary": 40000},  # +144%: rejected
            "Accountant": {"median_salary": 80000, "salary_10th": 55000},  # +4.6%
        },
    )
    with caplog.at_level(logging.INFO, logger="data_enrichment"):
        eng._enrich_bls_salary()
    written = {r["role"]: r for r in rows}
    assert "Registered Nurse" not in written  # previous value kept
    assert set(written) == {
        "Software Engineer",
        "Marketing Manager",
        "Sales Representative",
        "Truck Driver",
        "Warehouse Associate",
        "Customer Service Rep",
        "Accountant",
    }
    acct = written["Accountant"]
    assert acct["median_salary"] == LIVE_2025["13-2011"][1]
    assert acct["data_source"] == "BLS"
    meta = acct["metadata"]
    assert meta["soc_code"] == "13-2011" and meta["reference_year"] == _vintage()
    assert meta["series_ids"]["median"] == bls_oews.oews_series_id("13-2011", "13")
    assert marks[-1][1:] == (True, 7)
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "7/10 roles refreshed" in text
    assert "Registered Nurse (rejected, previous value kept" in text
    assert "Product Manager (no BLS occupation is this job" in text


# --------------------------------------------------------- the guards ---


def _ok(soc: str = "29-1141", median: int = 97550) -> Dict[str, Any]:
    return wage(soc, median, 68940, 137470, year=2025)


def test_guard_accepts_a_wage_within_the_band_and_one_with_no_previous() -> None:
    acc, rej = bls_oews.guard_refreshed_wages(
        {"RN": _ok(), "Chef": _ok("35-1011", 51000)}, {"RN": {"median_salary": 93600}}
    )
    assert set(acc) == {"RN", "Chef"} and rej == {}


def test_guard_rejects_a_series_that_is_not_the_socs_own() -> None:
    cross = _ok()
    cross["series_ids"] = dict(cross["series_ids"], median="OEUN000000000000015125213")
    acc, rej = bls_oews.guard_refreshed_wages({"RN": cross})
    assert acc == {} and "median series" in rej["RN"]


def test_guard_rejects_a_result_that_names_no_soc_or_period() -> None:
    bare = {"median": 82000, "p10": 59000, "source": "BLS OES"}
    no_period = {k: v for k, v in _ok().items() if k != "reference_period"}
    acc, rej = bls_oews.guard_refreshed_wages({"A": bare, "B": no_period})
    assert acc == {}
    assert "no SOC" in rej["A"] and "reference year / period" in rej["B"]


@pytest.mark.parametrize(
    "previous,rejected",
    [
        ({"median": 97550}, False),
        ({"median": 93600}, False),  # +4.2%: a normal year
        ({"median": 65000}, True),  # 97,550 is +50.08%
        ({"median": 60000}, True),  # +62.6%
        ({"median": 200000}, True),  # -51.2%
        ({"p90": 40000}, True),  # p90 137,470 vs 40,000
        ({"median_salary": 40000}, True),  # the Supabase column name
        ({"median": 0}, False),  # a zero previous is no previous
    ],
)
def test_guard_band_is_fifty_percent_either_way(previous: dict, rejected: bool) -> None:
    acc, rej = bls_oews.guard_refreshed_wages({"RN": _ok()}, {"RN": previous})
    assert ("RN" in rej) is rejected, (previous, rej)
    assert ("RN" in acc) is not rejected


def test_mixed_vintages_are_dropped_not_blended() -> None:
    ids = bls_oews.wage_series_ids("29-1141")
    series = [
        {
            "seriesID": ids["median"],
            "data": [{"year": "2025", "period": "A01", "value": "97550"}],
        },
        {
            "seriesID": ids["p10"],
            "data": [{"year": "2024", "period": "A01", "value": "66000"}],
        },
        {
            "seriesID": ids["p90"],
            "data": [{"year": "2025", "period": "A01", "value": "137470"}],
        },
        # newest-first is not assumed: the 2023 point must not beat 2025
        {
            "seriesID": ids["mean"],
            "data": [
                {"year": "2023", "period": "A01", "value": "90000"},
                {"year": "2025", "period": "A01", "value": "101420"},
            ],
        },
    ]
    result, _ = bls_oews.build_wage_result("29-1141", series)
    assert result is not None
    assert (result["median"], result["mean"], result["p90"]) == (97550, 101420, 137470)
    assert "p10" not in result  # another vintage: dropped
    assert result["reference_year"] == 2025


@pytest.mark.parametrize("raw", ["-", "*", ">=239200", "", None, "n/a"])
def test_suppressed_values_are_not_numbers(raw: Any) -> None:
    assert bls_oews.parse_wage_value(raw) is None
    ids = bls_oews.wage_series_ids("29-1141")
    series = [
        {
            "seriesID": ids["median"],
            "data": [{"year": "2025", "period": "A01", "value": raw}],
        }
    ]
    assert bls_oews.build_wage_result("29-1141", series)[0] is None


def test_values_with_thousands_separators_parse() -> None:
    assert bls_oews.parse_wage_value("135,980") == 135980
    assert bls_oews.parse_wage_value("94.5") == 94
