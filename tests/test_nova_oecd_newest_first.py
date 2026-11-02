"""OECD rows reach the model newest first, with the latest value per country.

Sweep 2026-11-03: query_oecd_sdmx returned 38 rows in API order (France
2003-01 first; Germany's 2026-09 value buried mid-list). Tool results are cut
to a few thousand characters before the model sees them, so "the current
unemployment rate" could be answered from 2003 data.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import api_enrichment  # noqa: E402
import nova  # noqa: E402

ROWS = [
    {"country": "France", "time_period": "2003-01", "value": 8.2},
    {"country": "Germany", "time_period": "2024-02", "value": 3.3},
    {"country": "France", "time_period": "2023-12", "value": 7.7},
    {"country": "Germany", "time_period": "2026-09", "value": 4.1},
    {"country": "France", "time_period": "2026-08", "value": 7.5},
]


def _run(cached=None):
    nv = nova.Nova.__new__(nova.Nova)
    fake = {"source": "OECD SDMX", "data": [dict(r) for r in ROWS], "row_count": len(ROWS)}
    with mock.patch.object(nova, "_upstash_get", lambda k: cached), mock.patch.object(
        nova, "_upstash_set", lambda *a, **k: None
    ), mock.patch.object(api_enrichment, "fetch_oecd_sdmx_data", lambda **k: dict(fake)):
        return nova.Nova._query_oecd_sdmx(nv, {"country": "DEU+FRA", "dataset": "unemployment_rate"})


def test_rows_newest_first_and_latest_summary() -> None:
    res = _run()
    periods = [r["time_period"] for r in res["data"]]
    assert periods == sorted(periods, reverse=True)
    assert res["latest"] == [
        {"country": "Germany", "time_period": "2026-09", "value": 4.1},
        {"country": "France", "time_period": "2026-08", "value": 7.5},
    ]
    # the summary leads the serialised result the model sees
    assert list(res)[0] == "latest"


def test_cached_result_is_ordered_too() -> None:
    cached = {"source": "OECD SDMX", "data": [dict(r) for r in ROWS]}
    res = _run(cached=cached)
    assert res["data"][0]["time_period"] == "2026-09"
    assert res["latest"][0]["country"] == "Germany"
