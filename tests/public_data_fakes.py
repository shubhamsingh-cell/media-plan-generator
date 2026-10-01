"""Hermetic fake network for the public-data-source tests.

Every outbound HTTP call the enrichment sources make (the pooled client in
``http_pool``, ``api_enrichment``'s and ``public_data_sources``' bound copies of
it, and ``urllib.request.urlopen``) is routed to the REAL responses recorded
under ``tests/fixtures/public_data_sources/`` (re-recorded with
``scripts/record_public_data_fixtures.py``). An URL the router does not know
raises ``UnmockedRequest`` (an ``OSError``, like the suite's external-network
guard) so no test can silently reach the internet.

The same router serves the OLD and the NEW client code: that is what lets the
regression tests fail on pre-fix source (it sees the real 302 / 301 / 404 the
providers send today) and pass on the fix.
"""

from __future__ import annotations

import io
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "public_data_sources"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class UnmockedRequest(OSError):
    """Raised for any URL the fake network has no recorded response for."""


@dataclass
class Req:
    url: str
    method: str = "GET"
    body: Optional[bytes] = None
    headers: Dict[str, str] = field(default_factory=dict)

    @property
    def parsed(self) -> urllib.parse.SplitResult:
        return urllib.parse.urlsplit(self.url)

    @property
    def host(self) -> str:
        return self.parsed.hostname or ""

    @property
    def path(self) -> str:
        return self.parsed.path

    @property
    def params(self) -> Dict[str, str]:
        return {k: v[0] for k, v in urllib.parse.parse_qs(self.parsed.query).items()}


@dataclass
class Resp:
    status: int = 200
    body: Any = b""
    headers: Dict[str, str] = field(default_factory=dict)

    def raw(self) -> bytes:
        if isinstance(self.body, (dict, list)):
            return json.dumps(self.body).encode("utf-8")
        if isinstance(self.body, str):
            return self.body.encode("utf-8")
        return self.body or b""


class _PooledResp:
    """The slice of http_pool._PooledResponse the sources use."""

    def __init__(self, resp: Resp) -> None:
        self.status = resp.status
        self.reason = ""
        self.headers = dict(resp.headers)
        self._data = resp.raw()

    def read(self) -> bytes:
        return self._data

    def getheader(self, name: str, default: Optional[str] = None) -> Optional[str]:
        for key, value in self.headers.items():
            if key.lower() == name.lower():
                return value
        return default


class _UrlopenResp:
    def __init__(self, resp: Resp, url: str) -> None:
        self.status = resp.status
        self.headers = dict(resp.headers)
        self._buf = io.BytesIO(resp.raw())
        self.url = url

    def __enter__(self) -> "_UrlopenResp":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def read(self) -> bytes:
        return self._buf.read()

    def getcode(self) -> int:
        return self.status


class FakeNet:
    """Router + recorder. ``requests`` lists every call that was made."""

    def __init__(self) -> None:
        self.requests: List[Req] = []
        self.latency_s: float = 0.0  # added to every datausa call (budget tests)
        self._overrides: List[Tuple[Callable[[Req], bool], Callable[[Req], Resp]]] = []
        self._fix: Dict[str, Any] = {}

    # -- introspection -----------------------------------------------------
    def count(self, host: str = "", path: str = "") -> int:
        return sum(
            1
            for r in self.requests
            if host in r.host and (not path or path in r.path)
        )

    def urls(self, host: str = "") -> List[str]:
        return [r.url for r in self.requests if host in r.host]

    # -- configuration -----------------------------------------------------
    def override(
        self, matcher: Callable[[Req], bool], responder: Callable[[Req], Resp]
    ) -> None:
        """Checked BEFORE the recorded world (last registered wins)."""
        self._overrides.insert(0, (matcher, responder))

    def down(self, host: str, status: int = 503) -> None:
        self.override(lambda r: host in r.host, lambda r: Resp(status, "unavailable"))

    # -- the recorded world ------------------------------------------------
    def _f(self, name: str) -> Any:
        if name not in self._fix:
            self._fix[name] = load(name)
        return self._fix[name]

    def handle(self, req: Req) -> Resp:
        self.requests.append(req)
        for matcher, responder in self._overrides:
            if matcher(req):
                return responder(req)
        host, path, q = req.host, req.path, req.params
        if host == "api.datausa.io":
            if self.latency_s:
                time.sleep(self.latency_s)
            return self._tesseract(path, q)
        if host == "datausa.io":
            fx = self._f("datausa_legacy_removed.json")
            return Resp(fx["status"], fx["body_head"], {"Content-Type": fx["content_type"]})
        if host == "api.census.gov":
            return self._census(q)
        if host == "restcountries.com":
            fx = self._f("restcountries_legacy_removed.json")
            return Resp(fx["status"], fx["redirect_body"], {"Location": fx["location"]})
        if host == "files-03.restcountries.com":
            return Resp(200, self._f("restcountries_legacy_removed.json")["followed_body"])
        if host == "sdmx.ilo.org":
            return self._ilo(path)
        if host == "api.beta.ons.gov.uk":
            fx = dict(self._f("ons_mgsx_trimmed.json"))
            fx.pop("_recorded_at", None)
            return Resp(200, fx)
        if host == "www150.statcan.gc.ca":
            if path.endswith("getDataFromVectorsAndLatestNPeriods"):
                return Resp(200, self._f("statcan_wds_unemployment.json")["response"])
            return Resp(404, "<html>not found</html>")
        if host == "api.worldbank.org":
            if "/GBR/" in path:
                return Resp(200, self._f("worldbank_pop_gbr.json")["response"])
            return Resp(200, [{"page": 1}, None])
        raise UnmockedRequest(f"no recorded response for {req.method} {req.url}")

    def _tesseract(self, path: str, q: Dict[str, str]) -> Resp:
        if path.endswith("/members"):
            if q.get("level") == "Year":
                fx = dict(self._f("datausa_members_year.json"))
                fx.pop("_recorded_at", None)
                return Resp(200, fx)
            by = self._f("datausa_members_place.json")["by_search"]
            return Resp(200, by.get(q.get("search", ""), {"members": []}))
        if path.endswith("/data.jsonrecords"):
            cuts = dict(c.split(":", 1) for c in q["include"].split(";"))
            level = q["drilldowns"]
            ids = set(cuts[level].split(","))
            year = cuts.get("Year", "")
            fixture = {
                "Place": "datausa_data_place_2024.json",
                "County": "datausa_data_county_2024.json",
                "State": "datausa_data_state_2024.json",
            }[level]
            fx = self._f(fixture)
            pop_cube = q["cube"] == "acs_yg_total_population_5"
            payload = dict(fx["population" if pop_cube else "income"])
            rows = [
                r
                for r in payload["data"]
                if r.get(f"{level} ID") in ids and year == "2024"
            ]
            return Resp(200, {**payload, "data": rows})
        raise UnmockedRequest(f"no recorded tesseract route for {path}")

    def _census(self, q: Dict[str, str]) -> Resp:
        fx = self._f("census_missing_key.json")
        key = q.get("key", "")
        if not key:
            return Resp(fx["status"], b"", {"Location": fx["location"], "Content-Length": "0"})
        if key == "BADKEY":
            return Resp(
                302,
                b"",
                {"Location": "https://api.census.gov/data/invalid_key.html"},
            )
        # a valid test key: the documented [header, rows...] shape
        return Resp(
            200,
            [
                ["NAME", "B01003_001E", "B19013_001E", "state"],
                ["Pennsylvania", "13018639", "76081", "42"],
                ["Kansas", "2947197", "74122", "20"],
                ["West Virginia", "1778373", "55948", "54"],
            ],
        )

    def _ilo(self, path: str) -> Resp:
        # /rest/data/ILO,<flow>/<key>
        parts = path.split("/")
        flow, key = parts[-2], parts[-1]
        if "DF_STI_ALL_UNE_DEA1_SEX_AGE_RT" in flow:
            fx = self._f("ilo_old_flow_404.json")
            return Resp(fx["status"], fx["body"])
        if not key.startswith("GBR."):
            return Resp(404, "NoRecordsFound")
        recorded = self._f("ilo_gbr_annual.json")
        if "EAP_DWAP" in key:
            return Resp(200, recorded["GBR:lfp"])
        if "Y15-24" in key:
            return Resp(200, recorded["GBR:youth_unemployment"])
        return Resp(200, recorded["GBR:unemployment"])

    # -- installation ------------------------------------------------------
    def install(self, monkeypatch: Any) -> "FakeNet":
        import api_enrichment
        import http_pool
        import public_data_sources

        def pooled(url: str, *, method: str = "GET", body: Optional[bytes] = None,
                   headers: Optional[Dict[str, str]] = None, timeout: float = 10.0,
                   ssl_ctx: Any = None) -> _PooledResp:
            return _PooledResp(self.handle(Req(url, method, body, dict(headers or {}))))

        def urlopen(target: Any, data: Any = None, timeout: Any = None, **kwargs: Any) -> _UrlopenResp:
            if isinstance(target, urllib.request.Request):
                req = Req(
                    target.full_url,
                    target.get_method(),
                    target.data,
                    {k: v for k, v in target.header_items()},
                )
            else:
                req = Req(str(target), "POST" if data else "GET", data)
            resp = self.handle(req)
            if resp.status >= 400:
                raise urllib.error.HTTPError(
                    req.url, resp.status, "fake", resp.headers, io.BytesIO(resp.raw())  # type: ignore[arg-type]
                )
            return _UrlopenResp(resp, req.url)

        monkeypatch.setattr(http_pool, "pooled_request", pooled)
        monkeypatch.setattr(api_enrichment, "_pooled_request", pooled, raising=False)
        monkeypatch.setattr(api_enrichment, "_HAS_POOL", True)
        monkeypatch.setattr(public_data_sources, "_pooled_request", pooled, raising=False)
        monkeypatch.setattr(public_data_sources, "_HAS_POOL", True)
        monkeypatch.setattr(urllib.request, "urlopen", urlopen)
        return self
