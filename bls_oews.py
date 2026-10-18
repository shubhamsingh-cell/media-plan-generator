"""BLS OEWS (survey code OE) series IDs, response parsing and refresh guards.

Why this module exists (2026-10-18). The scheduled BLS salary refresh logged
"0/10 roles refreshed" on 13 of 13 runs although BLS answered REQUEST_SUCCEEDED
every time, and no BLS wage ever reached a plan. Verified against the live BLS
public API (v2):

* the national series IDs the code built were VALID (``OEUN`` + area
  ``0000000`` + industry ``000000`` + 6-digit SOC + 2-digit datatype, 25 chars,
  all ``exists``), but
* OEWS publishes ONE vintage per series: observations carry only the latest
  reference year (``2025``, period ``A01`` = the May 2025 estimates) and every
  earlier year answers "No Data Available". The code asked for 2022-2024, got
  REQUEST_SUCCEEDED with empty series, and silently returned nothing.

The same query path also hid two builders that never produced a valid ID: the
metro lookup used a 5-digit area (23 chars, "Series does not exist") and the
``BLSClient`` per-role path used a 27-char ``OEUM...`` string. Every OEWS series
ID is built here, once, and validated to 25 characters.

Series-ID layout (download.bls.gov/pub/time.series/oe/oe.txt, oe.area,
oe.areatype, oe.datatype)::

    OE  U  N  0000000  000000  151252  13
    |   |  |  |        |       |       +- datatype (04 annual mean, 13 annual
    |   |  |  |        |       |          median, 11 / 15 annual 10th / 90th pct)
    |   |  |  |        |       +- SOC without the hyphen (000000 = all)
    |   |  |  |        +- industry (000000 = cross-industry)
    |   |  |  +- area: 0000000 national; metro 00 + 5-digit CBSA; state 7 digits
    |   |  +- areatype: N national, S state, M metro / nonmetro area
    |   +- seasonal (U)
    +- survey

Stdlib only: ``api_enrichment``, ``api_integrations``, ``data_enrichment`` and
``data_refresh`` all import it.
"""

from __future__ import annotations

import datetime
import logging
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

SERIES_ID_LENGTH = 25

AREATYPE_NATIONAL = "N"
AREATYPE_STATE = "S"
AREATYPE_METRO = "M"
NATIONAL_AREA = "0000000"
CROSS_INDUSTRY = "000000"
ALL_OCCUPATIONS = "000000"

# oe.datatype: 01 Employment, 03 Hourly mean, 04 Annual mean, 08 Hourly median,
# 11 Annual 10th pct, 12 Annual 25th, 13 Annual median, 14 Annual 75th,
# 15 Annual 90th pct.
DATATYPE_EMPLOYMENT = "01"
DATATYPE_ANNUAL_MEAN = "04"
DATATYPE_ANNUAL_P10 = "11"
DATATYPE_ANNUAL_MEDIAN = "13"
DATATYPE_ANNUAL_P90 = "15"

# result field -> annual-wage datatype
WAGE_DATATYPES: Dict[str, str] = {
    "mean": DATATYPE_ANNUAL_MEAN,
    "median": DATATYPE_ANNUAL_MEDIAN,
    "p10": DATATYPE_ANNUAL_P10,
    "p90": DATATYPE_ANNUAL_P90,
}

# A refreshed wage may differ from the one it replaces by at most this much
# (relative). OEWS moves a few percent a year; a wider jump is a wrong
# occupation, a wrong series or a bad parse, not a market move.
SANITY_BAND = 0.5

_SOC_RE = re.compile(r"^(\d{2})-?(\d{4})(?:\.\d{2})?$")


def normalize_soc(soc: Any) -> str:
    """``"NN-NNNN"`` for a SOC / O*NET-SOC string ("15-1252", "151252",
    "15-1252.00"), else ``""``. Anything that is not exactly a 6-digit SOC
    (e.g. "15125200", "15-12") is refused rather than trimmed."""
    m = _SOC_RE.match(str(soc or "").strip())
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def cbsa_area_code(cbsa: Any) -> str:
    """7-digit OEWS metro area code for a 5-digit CBSA ("35620" -> "0035620"),
    else ``""``."""
    digits = str(cbsa or "").strip()
    if not digits.isdigit() or len(digits) > 7:
        return ""
    return digits.zfill(7)


def oews_series_id(
    soc: Any,
    datatype: str,
    *,
    areatype: str = AREATYPE_NATIONAL,
    area: str = NATIONAL_AREA,
    industry: str = CROSS_INDUSTRY,
) -> str:
    """The 25-character OEWS series ID for one occupation and datatype.

    ``soc`` may be ``None`` / ``""`` for "all occupations" (``000000``).
    Raises ``ValueError`` for an invalid SOC, area or datatype so a malformed ID
    can never reach the API (BLS answers those "Series does not exist").
    """
    if soc in (None, ""):
        occupation = ALL_OCCUPATIONS
    else:
        norm = normalize_soc(soc)
        if not norm:
            raise ValueError(f"not a SOC code: {soc!r}")
        occupation = norm.replace("-", "")
    if not (len(datatype) == 2 and datatype.isdigit()):
        raise ValueError(f"bad OEWS datatype: {datatype!r}")
    if areatype not in (AREATYPE_NATIONAL, AREATYPE_STATE, AREATYPE_METRO):
        raise ValueError(f"bad OEWS areatype: {areatype!r}")
    if not (len(area) == 7 and area.isdigit()):
        raise ValueError(f"bad OEWS area code: {area!r}")
    if not (len(industry) == 6 and industry.isdigit()):
        raise ValueError(f"bad OEWS industry code: {industry!r}")
    series_id = f"OEU{areatype}{area}{industry}{occupation}{datatype}"
    if len(series_id) != SERIES_ID_LENGTH:  # pragma: no cover - guarded above
        raise ValueError(f"OEWS series id is not {SERIES_ID_LENGTH} chars: {series_id}")
    return series_id


def wage_series_ids(soc: Any) -> Dict[str, str]:
    """``{"mean": id, "median": id, "p10": id, "p90": id}`` for the national
    cross-industry annual wages of ``soc``."""
    return {field: oews_series_id(soc, dt) for field, dt in WAGE_DATATYPES.items()}


def year_window(
    this_year: Optional[int] = None, years_back: int = 3
) -> Tuple[str, str]:
    """``(startyear, endyear)`` for a BLS request that is certain to contain the
    published OEWS vintage.

    OEWS keeps only the newest reference year per series (2025 in October
    2026; the next arrives each spring), so the window must END at the current
    year and reach back far enough to cover a release that has not landed yet.
    The old fixed 2022-2024 window missed it entirely.
    """
    year = int(this_year) if this_year else datetime.datetime.now().year
    return str(year - years_back), str(year)


def parse_wage_value(raw: Any) -> Optional[int]:
    """Whole-dollar value of a BLS observation, ``None`` for a suppressed or
    non-numeric one ("-", "*", ">=239200")."""
    text = str(raw if raw is not None else "").strip().replace(",", "")
    if not re.fullmatch(r"\d+(?:\.\d+)?", text):
        return None
    return int(float(text))


def _point_key(point: Mapping[str, Any]) -> Tuple[int, str]:
    try:
        year = int(str(point.get("year") or "0"))
    except ValueError:
        year = 0
    return year, str(point.get("period") or "")


def newest_points(
    series_list: Iterable[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """``{seriesID: newest observation}`` for the series that carry data."""
    out: Dict[str, Dict[str, Any]] = {}
    for series in series_list or []:
        sid = str(series.get("seriesID") or "")
        points = [p for p in (series.get("data") or []) if isinstance(p, Mapping)]
        if sid and points:
            out[sid] = dict(max(points, key=_point_key))
    return out


def reference_label(year: int) -> str:
    """ "May 2025": OEWS estimates are for the May of their reference year
    (oe.release: "May 2004")."""
    return f"May {year}"


def build_wage_result(
    soc: str, series_list: Iterable[Mapping[str, Any]]
) -> Tuple[Optional[Dict[str, Any]], str]:
    """``(result, reason)`` from a BLS response's ``Results.series``.

    ``result`` carries mean / median / p10 / p90 (whole dollars), the SOC and
    series IDs they came from, and the reference year and period; ``None`` with
    a ``reason`` when the response holds no usable median or mean. Fields whose
    newest observation belongs to another year than the median's are dropped,
    never mixed.
    """
    norm = normalize_soc(soc)
    if not norm:
        return None, f"not a SOC code: {soc!r}"
    ids = wage_series_ids(norm)
    points = newest_points(series_list)
    wanted = {sid: field for field, sid in ids.items()}
    found = {wanted[sid]: pt for sid, pt in points.items() if sid in wanted}
    anchor = found.get("median") or found.get("mean")
    if not anchor:
        return None, "no observation for the national median or mean series"
    year = _point_key(anchor)[0]
    values: Dict[str, int] = {}
    for field, point in found.items():
        if _point_key(point)[0] != year:
            continue
        value = parse_wage_value(point.get("value"))
        if value is not None:
            values[field] = value
    if "median" not in values and "mean" not in values:
        return None, "median and mean are suppressed or non-numeric"
    result: Dict[str, Any] = {
        "source": "BLS OES",
        **values,
        "soc_code": norm,
        "reference_year": year,
        "reference_period": reference_label(year),
        "series_ids": {f: ids[f] for f in values},
    }
    return result, ""


def series_mismatch(result: Mapping[str, Any]) -> str:
    """ "" when every series ID in ``result`` is the national annual-wage series
    of the SOC the result names; otherwise a description of the cross-wiring."""
    soc = normalize_soc(result.get("soc_code"))
    if not soc:
        return "result names no SOC"
    expected = wage_series_ids(soc)
    ids = result.get("series_ids")
    if not isinstance(ids, Mapping) or not ids:
        return "result names no series"
    for field, sid in ids.items():
        if expected.get(field) != sid:
            return f"{field} series {sid} is not {expected.get(field)} (SOC {soc})"
    for field in WAGE_DATATYPES:
        if result.get(field) is not None and field not in ids:
            return f"{field} has no series id"
    return ""


def _previous_value(previous: Mapping[str, Any], field: str) -> Optional[float]:
    aliases = {
        "median": ("median", "median_salary"),
        "mean": ("mean",),
        "p10": ("p10", "salary_10th"),
        "p90": ("p90", "salary_90th"),
    }
    for key in aliases[field]:
        try:
            value = float(previous.get(key))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def guard_refreshed_wages(
    refreshed: Mapping[str, Mapping[str, Any]],
    previous: Optional[Mapping[str, Mapping[str, Any]]] = None,
    *,
    band: float = SANITY_BAND,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """``(accepted, rejected)``: which refreshed wages may replace the stored ones.

    A role is accepted only when (1) its series IDs are the national wage
    series of the SOC it carries, (2) it names its reference year and period,
    and (3) no wage differs from the previous stored one by more than ``band``
    (relative). A rejected role keeps its previous value (the caller must not
    overwrite it) and is logged at WARNING with the reason.
    """
    accepted: Dict[str, Dict[str, Any]] = {}
    rejected: Dict[str, str] = {}
    for role, result in (refreshed or {}).items():
        if not isinstance(result, Mapping):
            continue
        problem = series_mismatch(result)
        if not problem and not (
            result.get("reference_year") and result.get("reference_period")
        ):
            problem = "result carries no reference year / period"
        if not problem:
            prev = (previous or {}).get(role)
            if isinstance(prev, Mapping):
                for field in WAGE_DATATYPES:
                    new_v = result.get(field)
                    old_v = _previous_value(prev, field)
                    if new_v is None or old_v is None:
                        continue
                    change = float(new_v) / old_v - 1.0
                    if abs(change) > band:
                        problem = (
                            f"{field} {new_v} vs previous {old_v:.0f} "
                            f"({change:+.0%}) is outside +/-{band:.0%}"
                        )
                        break
        if problem:
            rejected[role] = problem
            logger.warning(
                "BLS refresh for %r rejected, previous value kept: %s", role, problem
            )
        else:
            accepted[role] = dict(result)
    return accepted, rejected


def describe_skips(skips: Mapping[str, str]) -> str:
    """ "; "-joined ``role (reason)`` text for a refresh log line."""
    if not skips:
        return ""
    return "; ".join(f"{role} ({reason})" for role, reason in skips.items())
