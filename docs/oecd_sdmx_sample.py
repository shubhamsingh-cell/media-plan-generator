"""Standalone OECD SDMX API client -- discovery spike sample.

NOT YET INTEGRATED INTO nova.py. This is a reference implementation for the
integration plan in docs/OECD_SDMX_Discovery_2026.md.

The OECD migrated from stats.oecd.org (legacy SDMX-JSON) to sdmx.oecd.org
(OECD Data Explorer API) in 2024. The legacy host now returns HTTP 405 / 28
(timeout). This module targets the new endpoint exclusively.

Public API (single function):
    query_oecd(country, dataset, start_year=None, end_year=None) -> dict

Run as a script for a working demo:
    python docs/oecd_sdmx_sample.py
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

OECD_BASE_URL = "https://sdmx.oecd.org/public/rest/data"
DEFAULT_TIMEOUT_SECS = 30
DEFAULT_USER_AGENT = (
    "NovaAISuite/1.0 (OECD-SDMX-spike; contact: shubhamsingh@joveo.com)"
)

# Recruitment-relevant dataset catalogue. Each entry is a complete spec needed
# to build a query: agency, dataflow, version, dimension key template.
#
# The dimension key MUST have the exact number of "." separators required by
# the dataflow's Data Structure Definition. {country} placeholder is replaced
# with the ISO-3 country code. "" between dots = "all values" for that dim.
#
# Verified live 2026-05-22 against sdmx.oecd.org.
OECD_DATASETS: dict[str, dict[str, Any]] = {
    "unemployment_rate_monthly": {
        "agency": "OECD.SDD.TPS",
        "dataflow": "DSD_LFS@DF_IALFS_UNE_M",
        "version": "1.0",
        # 9 dims: REF_AREA . MEASURE . UNIT_MEASURE . TRANSFORMATION
        #         . ADJUSTMENT . SEX . AGE . ACTIVITY . FREQ
        "key_template": "{country}.UNE_LF_M.PT_LF_SUB._Z.Y._T.Y_GE15._Z.M",
        "freq": "M",
        "unit": "percentage of labour force",
        "description": "Monthly harmonised unemployment rate, 15+, seasonally adjusted",
    },
    "labour_force_indicators": {
        "agency": "OECD.SDD.TPS",
        "dataflow": "DSD_LFS@DF_IALFS_INDIC",
        "version": "1.0",
        # 9 dims; leaving most blank pulls all measures (EMP, WAP, etc.).
        # Use this when you want a broad pull and will filter client-side.
        "key_template": "{country}........",
        "freq": "Q",
        "unit": "thousands of persons",
        "description": "Quarterly labour force indicators: employment, working-age population, activity by sex/age/industry",
    },
    "average_annual_wages": {
        "agency": "OECD.ELS.SAE",
        "dataflow": "DSD_EARNINGS@AV_AN_WAGE",
        "version": "1.0",
        # 7 dims: REF_AREA . MEASURE . UNIT_MEASURE . PAY_PERIOD
        #         . PRICE_BASE . AGGREGATION_OPERATION . SEX
        "key_template": "{country}......",
        "freq": "A",
        "unit": "USD PPP, constant prices",
        "description": "Average annual wages per full-time-equivalent employee",
    },
    "hours_worked": {
        "agency": "OECD.ELS.SAE",
        "dataflow": "DSD_HW@DF_AVG_USL_WK_WKD",
        "version": "1.0",
        # 13 dims -- leave all but country empty for full pull
        "key_template": "{country}............",
        "freq": "A",
        "unit": "hours per week per person",
        "description": "Average usual weekly hours worked in main job",
    },
    "productivity_levels": {
        "agency": "OECD.SDD.TPS",
        "dataflow": "DSD_PDB@DF_PDB_LV",
        "version": "1.0",
        # 9 dims
        "key_template": "{country}........",
        "freq": "A",
        "unit": "various (GDP per hour, per person employed, per capita)",
        "description": "Productivity levels: GDP per hour worked, per person employed, per capita",
    },
    "migration_inflows": {
        "agency": "OECD.ELS.IMD",
        "dataflow": "DSD_MIG@DF_MIG",
        "version": "1.0",
        # 8 dims: REF_AREA . CITIZENSHIP . FREQ . MEASURE . SEX
        #         . BIRTH_PLACE . EDUCATION_LEV . UNIT_MEASURE
        # IMPORTANT: full pull is ~14MB. Always filter by country.
        "key_template": "{country}.......",
        "freq": "A",
        "unit": "persons",
        "description": "International migration: inflows, outflows, asylum seekers, nationality acquisitions",
    },
}


def _build_url(
    agency: str,
    dataflow: str,
    version: str,
    key: str,
    start_year: int | str | None = None,
    end_year: int | str | None = None,
) -> str:
    """Build an OECD SDMX REST URL.

    Pattern: {base}/{agency},{dataflow},{version}/{key}?startPeriod=&endPeriod=
    """
    path = f"{OECD_BASE_URL}/{agency},{dataflow},{version}/{key}"
    params: list[tuple[str, str]] = []
    if start_year is not None:
        params.append(("startPeriod", str(start_year)))
    if end_year is not None:
        params.append(("endPeriod", str(end_year)))
    if params:
        path = f"{path}?{urllib.parse.urlencode(params)}"
    return path


def _fetch_sdmx_json(url: str, timeout: int = DEFAULT_TIMEOUT_SECS) -> dict[str, Any]:
    """GET an OECD SDMX-JSON v2 response.

    Returns parsed JSON. Raises urllib.error.HTTPError on non-2xx,
    json.JSONDecodeError on bad payload, urllib.error.URLError on network failure.
    """
    req = urllib.request.Request(
        url,
        headers={
            # The new API rejects requests without an explicit SDMX content type.
            # vendor type below is what works in 2026-05.
            "Accept": "application/vnd.sdmx.data+json; version=2",
            "User-Agent": DEFAULT_USER_AGENT,
        },
    )
    started = time.monotonic()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    elapsed_ms = int((time.monotonic() - started) * 1000)
    payload = json.loads(raw.decode("utf-8"))
    payload["_meta_fetch"] = {"url": url, "elapsed_ms": elapsed_ms, "bytes": len(raw)}
    return payload


def _flatten_sdmx_json(
    payload: dict[str, Any],
    country: str,
    dataset_key: str,
) -> list[dict[str, Any]]:
    """Convert SDMX-JSON 2.0 dataSets/series/observations into a flat list.

    SDMX-JSON encodes dimensions by integer index into structures[0].dimensions.observation.
    This walks the series dict (keys like "0:0:0:0:0:0:0:0:0") and resolves each
    integer back to its dimension value's "id" and "name".

    Returns a list of dicts ready for the chatbot context. Each row has:
        country, time_period, value, measure, unit, freq, dataset, source
    """
    flat: list[dict[str, Any]] = []
    try:
        data = payload.get("data") or {}
        datasets = data.get("dataSets") or []
        structures = (
            payload.get("data", {}).get("structures") or payload.get("structures") or []
        )
        if not datasets or not structures:
            return flat

        struct = structures[0]
        dims_series = (struct.get("dimensions") or {}).get("series") or []
        dims_obs = (struct.get("dimensions") or {}).get("observation") or []

        def _resolve(dim_list: list[dict[str, Any]], idx_str: str) -> dict[str, str]:
            """idx_str like '0:0:0:0:0' -> dict of dim_id -> value name."""
            out: dict[str, str] = {}
            parts = idx_str.split(":")
            for i, dim in enumerate(dim_list):
                if i >= len(parts):
                    break
                pos = int(parts[i])
                values = dim.get("values") or []
                if 0 <= pos < len(values):
                    val = values[pos]
                    out[dim.get("id", f"dim{i}")] = val.get("name") or val.get("id", "")
            return out

        for ds in datasets:
            series = ds.get("series") or {}
            for series_key, series_val in series.items():
                series_dims = _resolve(dims_series, series_key)
                observations = series_val.get("observations") or {}
                for obs_key, obs_val in observations.items():
                    obs_dims = _resolve(dims_obs, obs_key)
                    value = obs_val[0] if obs_val else None
                    row: dict[str, Any] = {
                        "country": series_dims.get("REF_AREA") or country,
                        "time_period": obs_dims.get("TIME_PERIOD", ""),
                        "value": value,
                        "measure": series_dims.get("MEASURE", ""),
                        "unit": series_dims.get("UNIT_MEASURE", ""),
                        "freq": series_dims.get("FREQ") or obs_dims.get("FREQ", ""),
                        "dataset": dataset_key,
                        "source": "OECD SDMX",
                    }
                    flat.append(row)
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        logger.error(f"SDMX-JSON flatten failed: {exc}", exc_info=True)
    return flat


def query_oecd(
    country: str,
    dataset: str,
    start_year: int | str | None = None,
    end_year: int | str | None = None,
    *,
    timeout: int = DEFAULT_TIMEOUT_SECS,
) -> dict[str, Any]:
    """Query OECD SDMX for a recruitment-relevant indicator.

    Args:
        country: ISO-3 country code (e.g. "USA", "GBR", "DEU"). Multi-country
            via "+" join is supported (e.g. "USA+GBR+DEU").
        dataset: Key from OECD_DATASETS catalogue. See module docstring or call
            with dataset="?" to list options.
        start_year: e.g. 2023, "2024-01", "2024-Q1". None = no lower bound.
        end_year: e.g. 2026, "2026-12". None = latest available.
        timeout: HTTP timeout in seconds.

    Returns:
        {
          "tool": "query_oecd_sdmx",
          "source": "OECD SDMX",
          "dataset": <dataset key>,
          "country": <country>,
          "url": <full request URL for debugging>,
          "elapsed_ms": <int>,
          "row_count": <int>,
          "rows": [...flat dicts...],  # truncated to first 500
          "description": <human-readable dataset description>,
        }
        On error: same shape with "error" key, "rows" empty.
    """
    country = (country or "").upper().strip()
    dataset_key = (dataset or "").lower().strip()

    if dataset_key == "?" or not dataset_key:
        return {
            "tool": "query_oecd_sdmx",
            "source": "OECD SDMX",
            "available_datasets": list(OECD_DATASETS.keys()),
            "hint": "Call query_oecd(country='USA', dataset='unemployment_rate_monthly')",
        }

    if dataset_key not in OECD_DATASETS:
        return {
            "tool": "query_oecd_sdmx",
            "source": "OECD SDMX",
            "error": f"Unknown dataset '{dataset_key}'. Available: {list(OECD_DATASETS.keys())}",
            "rows": [],
        }

    if not country:
        return {
            "tool": "query_oecd_sdmx",
            "source": "OECD SDMX",
            "error": "country is required (ISO-3 code, e.g. 'USA' or 'USA+GBR+DEU')",
            "rows": [],
        }

    spec = OECD_DATASETS[dataset_key]
    key = spec["key_template"].format(country=country)
    url = _build_url(
        agency=spec["agency"],
        dataflow=spec["dataflow"],
        version=spec["version"],
        key=key,
        start_year=start_year,
        end_year=end_year,
    )

    try:
        payload = _fetch_sdmx_json(url, timeout=timeout)
    except urllib.error.HTTPError as exc:
        # 404 = NoRecordsFound (legitimate empty result), 422 = bad dim count
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        return {
            "tool": "query_oecd_sdmx",
            "source": "OECD SDMX",
            "dataset": dataset_key,
            "country": country,
            "url": url,
            "error": f"HTTP {exc.code}: {body or exc.reason}",
            "rows": [],
        }
    except urllib.error.URLError as exc:
        return {
            "tool": "query_oecd_sdmx",
            "source": "OECD SDMX",
            "dataset": dataset_key,
            "country": country,
            "url": url,
            "error": f"Network error: {exc.reason}",
            "rows": [],
        }
    except (json.JSONDecodeError, ValueError) as exc:
        return {
            "tool": "query_oecd_sdmx",
            "source": "OECD SDMX",
            "dataset": dataset_key,
            "country": country,
            "url": url,
            "error": f"Bad SDMX-JSON: {exc}",
            "rows": [],
        }

    fetch_meta = payload.get("_meta_fetch") or {}
    rows = _flatten_sdmx_json(payload, country=country, dataset_key=dataset_key)
    return {
        "tool": "query_oecd_sdmx",
        "source": "OECD SDMX",
        "dataset": dataset_key,
        "country": country,
        "url": url,
        "elapsed_ms": fetch_meta.get("elapsed_ms"),
        "bytes": fetch_meta.get("bytes"),
        "row_count": len(rows),
        # Truncate to 500 rows for chatbot context; full payload available via url
        "rows": rows[:500],
        "description": spec["description"],
        "freq": spec["freq"],
        "unit": spec["unit"],
    }


def _demo() -> None:
    """Demo: query monthly unemployment rate for G7 countries, latest 3 months."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    g5 = "USA+GBR+DEU+FRA+JPN"  # G7 minus ITA/CAN -- 7 countries hits payload limits
    print(f"\n=== OECD SDMX demo: monthly unemployment for G5 ({g5}) ===\n")
    result = query_oecd(
        country=g5,
        dataset="unemployment_rate_monthly",
        start_year="2026-01",
        end_year="2026-04",
        timeout=60,
    )
    print(f"URL: {result.get('url')}")
    print(f"HTTP: {result.get('elapsed_ms')}ms, {result.get('bytes')} bytes")
    if result.get("error"):
        print(f"ERROR: {result['error']}")
        return
    print(f"Rows: {result['row_count']}")
    print(f"Description: {result['description']}\n")
    # Print first 12 rows grouped by country
    for row in result["rows"][:12]:
        print(
            f"  {row['country']:30s}  {row['time_period']:10s}  "
            f"{row['value']:>6}  {row['unit']}"
        )
    if result["row_count"] > 12:
        print(f"  ... and {result['row_count'] - 12} more rows")

    # Bonus: average annual wages, latest year
    print("\n=== Bonus: average annual wages (USA, latest 4 years) ===\n")
    wages = query_oecd(
        country="USA",
        dataset="average_annual_wages",
        start_year=2020,
        end_year=2023,
        timeout=60,
    )
    print(f"URL: {wages.get('url')}")
    print(f"Rows: {wages.get('row_count')}")
    for row in wages.get("rows", [])[:8]:
        print(
            f"  {row['country']:20s}  {row['time_period']:6s}  "
            f"{row['value']:>12}  {row['unit']}"
        )


if __name__ == "__main__":
    _demo()
