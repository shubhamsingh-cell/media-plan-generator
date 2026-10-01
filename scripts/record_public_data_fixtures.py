#!/usr/bin/env python3
"""Re-record the small public-API fixtures under tests/fixtures/public_data_sources/.

The data-source repair (branch mpg-data-sources) probes each public provider
LIVE and freezes a few trimmed rows of the REAL response so the unit tests
exercise the real wire shape without touching the network. Run this when a
provider changes shape and the tests need re-baselining:

    python3 scripts/record_public_data_fixtures.py

Every request below is keyless. Census ACS (api.census.gov/data/...) is NOT
recorded as data because it now requires a key on every query -- the recorded
artefact there is the 302 -> missing_key.html redirect itself.
"""

from __future__ import annotations

import http.client
import json
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "public_data_sources"
UA = "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)"
TESS = "https://api.datausa.io/tesseract"

# The real production locations from the 2026-09 telemetry (small US places).
# Same-name / look-alike place searches recorded to pin the matcher's wrong-place
# guards (Wayne PA must never resolve to Wayne Heights; Burbank / Mountain View /
# Franklin carry county-qualified siblings; Louisville / Indianapolis are the
# legal-name suffix conventions).
NAME_CASES = [
    "Wayne", "Lancaster", "Columbia", "Franklin", "Springfield", "Louisville",
    "Indianapolis", "Burbank", "Mountain View", "Nashville",
]

PLACES = {
    "Hershey": "PA",
    "Edgerton": "KS",
    "Hazleton": "PA",
    "Lancaster": "PA",
    "Wheeling": "WV",
    # The look-alike the matcher must NEVER pick for "Wayne, PA": its figures are
    # recorded so a regression would publish them (3,293) instead of failing quietly.
    "Wayne Heights": "PA",
}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a: Any, **k: Any) -> None:  # noqa: D401
        return None


def _get(url: str, *, accept: str = "application/json", follow: bool = True) -> Tuple[int, Dict[str, str], bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept})
    opener = urllib.request.build_opener() if follow else urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(req, timeout=30) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def _post(url: str, body: Any) -> Tuple[int, bytes]:
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"User-Agent": UA, "Accept": "application/json", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, resp.read()


def _tess(path: str, **params: str) -> Dict[str, Any]:
    qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote, safe=":,;")
    status, _h, body = _get(f"{TESS}/{path}?{qs}")
    if status != 200:
        raise SystemExit(f"{path} {params} -> HTTP {status}")
    return json.loads(body.decode())


def _write(name: str, obj: Any) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    obj = {"_recorded_at": time.strftime("%Y-%m-%d"), **obj} if isinstance(obj, dict) else obj
    (OUT / name).write_text(json.dumps(obj, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print("wrote", name)


def record_datausa() -> None:
    members: Dict[str, Any] = {}
    for city, st in PLACES.items():
        members[city] = _tess("members", cube="acs_yg_total_population_5", level="Place", search=city)
    members["Zzqxville"] = _tess("members", cube="acs_yg_total_population_5", level="Place", search="Zzqxville")
    for name in NAME_CASES:
        members.setdefault(name, _tess("members", cube="acs_yg_total_population_5", level="Place", search=name))
    _write("datausa_members_place.json", {"by_search": members})
    _write("datausa_members_year.json", _tess("members", cube="acs_yg_total_population_5", level="Year"))

    ids = {}
    for city, st in PLACES.items():
        cap = f"{city}, {st}"
        for m in members[city]["members"]:
            if m["caption"] == cap:
                ids[cap] = m["key"]
    id_list = ",".join(ids.values())
    year = max(m["key"] for m in _tess("members", cube="acs_yg_total_population_5", level="Year")["members"])

    def data(cube: str, level: str, measure: str, idl: str, race: bool = False) -> Dict[str, Any]:
        inc = f"{level}:{idl};Year:{year}" + (";Race:0" if race else "")
        return _tess("data.jsonrecords", cube=cube, drilldowns=level, measures=measure, include=inc)

    _write(
        "datausa_data_place_2024.json",
        {
            "year": year,
            "ids": ids,
            "population": data("acs_yg_total_population_5", "Place", "Population", id_list),
            "income": data("acs_ygr_median_household_income_race_5", "Place", "Household Income by Race", id_list, race=True),
        },
    )
    _write(
        "datausa_data_county_2024.json",
        {
            "population": data("acs_yg_total_population_5", "County", "Population", "05000US42043,05000US42029"),
            "income": data("acs_ygr_median_household_income_race_5", "County", "Household Income by Race", "05000US42043,05000US42029", race=True),
        },
    )
    st_ids = "04000US42,04000US20,04000US54"
    _write(
        "datausa_data_state_2024.json",
        {
            "population": data("acs_yg_total_population_5", "State", "Population", st_ids),
            "income": data("acs_ygr_median_household_income_race_5", "State", "Household Income by Race", st_ids, race=True),
        },
    )
    status, _h, body = _get("https://datausa.io/api/data?drilldowns=State&measures=Population&year=latest")
    _write("datausa_legacy_removed.json", {"status": status, "content_type": _h.get("Content-Type"), "body_head": body.decode("utf-8", "replace")[:160]})


def record_census_and_restcountries() -> None:
    status, headers, body = _get(
        "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01003_001E,B19013_001E&for=state:*", follow=False
    )
    _write(
        "census_missing_key.json",
        {"status": status, "location": headers.get("Location"), "x_datawebapi_keyerror": headers.get("X-DataWebAPI-KeyError"), "content_length": headers.get("Content-Length"), "body": body.decode("utf-8", "replace")},
    )
    status, headers, body = _get("https://restcountries.com/v3.1/alpha/GBR?fields=name,population", follow=False)
    _s2, _h2, body2 = _get("https://restcountries.com/v3.1/alpha/GBR?fields=name,population", follow=True)
    _write(
        "restcountries_legacy_removed.json",
        {"status": status, "location": headers.get("Location"), "redirect_body": body.decode("utf-8", "replace")[:200], "followed_status": _s2, "followed_body": json.loads(body2.decode())},
    )


def record_ilo() -> None:
    base = "https://sdmx.ilo.org/rest/data"
    series = {
        "unemployment": "ILO,DF_UNE_DEAP_SEX_AGE_RT/{c}.A.UNE_DEAP_RT.SEX_T.AGE_YTHADULT_YGE15",
        "youth_unemployment": "ILO,DF_UNE_DEAP_SEX_AGE_RT/{c}.A.UNE_DEAP_RT.SEX_T.AGE_YTHADULT_Y15-24",
        "lfp": "ILO,DF_EAP_DWAP_SEX_AGE_RT/{c}.A.EAP_DWAP_RT.SEX_T.AGE_YTHADULT_YGE15",
    }
    out: Dict[str, Any] = {}
    for iso in ("GBR",):
        for name, path in series.items():
            status, _h, body = _get(f"{base}/{path.format(c=iso)}?format=jsondata&startPeriod=2023")
            if status != 200:
                raise SystemExit(f"ILO {iso} {name} -> {status}")
            d = json.loads(body.decode())
            # trim the (large) structure description text; keep dimensions + values
            for s in d["data"].get("structures", []):
                for noisy in ("description", "descriptions", "names", "annotations", "attributes"):
                    s.pop(noisy, None)
                for axis in ("series", "observation"):
                    s["dimensions"][axis] = [
                        {
                            "id": dim["id"],
                            "values": [{"id": v["id"], "name": v.get("name")} for v in dim["values"]],
                        }
                        for dim in s["dimensions"].get(axis, [])
                    ]
                s["dimensions"].pop("dataSet", None)
            out[f"{iso}:{name}"] = d
    _write("ilo_gbr_annual.json", out)
    status, _h, body = _get(f"{base}/ILO,DF_STI_ALL_UNE_DEA1_SEX_AGE_RT/USA.A......?format=jsondata&startPeriod=2022&detail=dataonly")
    _write("ilo_old_flow_404.json", {"status": status, "body": body.decode("utf-8", "replace")[:200]})


def record_ons_statcan_worldbank() -> None:
    uri = "/employmentandlabourmarket/peoplenotinwork/unemployment/timeseries/mgsx/lms"
    status, _h, body = _get("https://api.beta.ons.gov.uk/v1/data?uri=" + urllib.parse.quote(uri, safe=""))
    d = json.loads(body.decode())
    keep = {k: d.get(k) for k in ("description", "uri")}
    for k in ("months", "quarters", "years"):
        keep[k] = (d.get(k) or [])[-3:]
    _write("ons_mgsx_trimmed.json", keep)

    status, body2 = _post("https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods", [{"vectorId": 2062815, "latestN": 3}])
    _write("statcan_wds_unemployment.json", {"response": json.loads(body2.decode())})

    status, _h, body3 = _get("https://api.worldbank.org/v2/country/GBR/indicator/SP.POP.TOTL?format=json&per_page=5&date=2019:2024")
    _write("worldbank_pop_gbr.json", {"response": json.loads(body3.decode())})


def main() -> int:
    record_datausa()
    record_census_and_restcountries()
    record_ilo()
    record_ons_statcan_worldbank()
    return 0


if __name__ == "__main__":
    sys.exit(main())
