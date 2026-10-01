"""Keyless public-data clients for the enrichment pipeline (stdlib only).

Why this module exists (production telemetry 2026-09-23..30, every plan):

* ``api.census.gov`` now answers EVERY data query without a key with a
  ``302 -> /data/missing_key.html`` (header ``X-DataWebAPI-KeyError: 1``). The
  old client followed no redirects, read the empty 302 body and logged
  ``Expecting value`` -- three times (one per ACS vintage, 10 s each).
* ``datausa.io/api/data`` (legacy) was removed; the live API is the Tesseract
  service at ``api.datausa.io/tesseract`` (same ACS 5-year tables, keyless, and
  it serves PLACE-level rows -- the old code only ever asked for states and then
  stamped the state figure onto one city).
* ``restcountries.com`` v1-v4 were switched off (301 -> a static "deprecated"
  JSON); v5 needs an account + bearer key.
* The ILO SDMX dataflow ``DF_STI_ALL_UNE_DEA1_SEX_AGE_RT`` no longer exists (404);
  the annual unemployment / participation flows are ``DF_UNE_DEAP_SEX_AGE_RT``
  and ``DF_EAP_DWAP_SEX_AGE_RT``.

Design rules (CLAUDE.md: stdlib only, error isolation per source):

* ``request_json`` is SINGLE-SHOT with a hard per-call timeout and classifies
  every failure (``redirect`` / ``http`` / ``not_json`` / ``timeout`` /
  ``network``) instead of swallowing it as ``None``.
* A source that had applicable work but got NO data raises ``SourceFailure``;
  the enrichment ``_safe_call`` wrapper maps an exception to the "failed"
  status, an empty ``{}`` to "not applicable" and a populated dict to "data".
* Every multi-geography fetch runs its independent calls in a small pool and
  honours a total wall-clock ``budget_s`` -- stragglers are abandoned, never
  awaited.
"""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import logging
import re
import socket
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

try:  # pooled keep-alive connections (stdlib-only module of this repo)
    from http_pool import pooled_request as _pooled_request

    _HAS_POOL = True
except ImportError:  # pragma: no cover - the repo always ships http_pool
    _HAS_POOL = False

logger = logging.getLogger("public_data_sources")

USER_AGENT = "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)"
#: Hard cap for ONE HTTP call. A source budget is enforced on top of this.
CALL_TIMEOUT_S = 6.0
#: Wall-clock budget for the whole US-demographics source (all geographies).
DEMOGRAPHICS_BUDGET_S = 8.0
#: Wall-clock budget for the ILO source.
ILO_BUDGET_S = 8.0


# ---------------------------------------------------------------------------
# Failure vocabulary
# ---------------------------------------------------------------------------


class FetchError(Exception):
    """One classified HTTP/JSON failure.

    ``kind`` is one of: ``redirect`` (3xx, e.g. Census missing-key),
    ``http`` (>= 400), ``not_json`` (200 but not JSON), ``timeout``,
    ``network`` and ``schema`` (JSON parsed but not the expected shape).
    """

    def __init__(
        self,
        kind: str,
        detail: str = "",
        status: Optional[int] = None,
        location: str = "",
    ) -> None:
        super().__init__(f"{kind}: {detail}" if detail else kind)
        self.kind = kind
        self.detail = detail
        self.status = status
        self.location = location


class SourceFailure(Exception):
    """A source had applicable work for this plan but obtained no data.

    Raised (never swallowed into ``{}``) so the enrichment wrapper reports the
    source as FAILED rather than "not applicable".
    """


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def _raw_request(
    url: str,
    method: str,
    body: Optional[bytes],
    headers: Dict[str, str],
    timeout: float,
) -> Tuple[int, Dict[str, str], bytes]:
    """One HTTP request, no redirects followed. Returns (status, headers, body)."""
    if _HAS_POOL:
        resp = _pooled_request(
            url, method=method, body=body, headers=headers, timeout=timeout
        )
        return resp.status, {k: v for k, v in resp.headers.items()}, resp.read()

    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args: Any, **kwargs: Any) -> None:
            return None

    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def request_json(
    url: str,
    *,
    method: str = "GET",
    body: Any = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = CALL_TIMEOUT_S,
) -> Any:
    """Single-shot JSON request with classified failures (never returns None).

    Raises ``FetchError``. The URL is deliberately NOT included in the error
    text: some callers put an API key in the query string and the logs must
    never carry it.
    """
    all_headers: Dict[str, str] = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    if headers:
        all_headers.update(headers)
    payload: Optional[bytes] = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        all_headers.setdefault("Content-Type", "application/json")

    try:
        status, resp_headers, raw = _raw_request(
            url, method, payload, all_headers, float(timeout)
        )
    except (socket.timeout, TimeoutError) as exc:
        raise FetchError("timeout", str(exc) or "timed out") from exc
    except (OSError, http.client.HTTPException) as exc:
        raise FetchError("network", f"{type(exc).__name__}: {exc}") from exc

    location = ""
    for key, value in (resp_headers or {}).items():
        if str(key).lower() == "location":
            location = str(value)
    if 300 <= status < 400:
        raise FetchError("redirect", f"HTTP {status}", status, location)
    if status >= 400:
        raise FetchError("http", f"HTTP {status}", status)
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise FetchError("not_json", f"HTTP {status} body is not JSON", status) from exc


# ---------------------------------------------------------------------------
# Tiny TTL cache (static reference lookups: place ids, latest ACS year)
# ---------------------------------------------------------------------------

_CACHE: Dict[str, Tuple[float, Any]] = {}
_CACHE_LOCK = threading.Lock()


def _cache_get(key: str, ttl: float) -> Any:
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit and time.time() - hit[0] < ttl:
            return hit[1]
    return None


def _cache_put(key: str, value: Any) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), value)


def clear_caches() -> None:
    """Drop every module cache (tests; also useful after a vintage flip)."""
    with _CACHE_LOCK:
        _CACHE.clear()
    with _CENSUS_STATE_LOCK:
        _CENSUS_STATE.clear()


# ---------------------------------------------------------------------------
# Bounded parallel runner
# ---------------------------------------------------------------------------


def run_parallel(
    jobs: Sequence[Tuple[str, Callable[[], Any]]],
    *,
    max_workers: int,
    deadline: float,
) -> Dict[str, Any]:
    """Run ``jobs`` ([(name, fn)]) in a small pool until ``deadline``.

    Returns {name: result-or-Exception}. A job still running at the deadline
    gets ``TimeoutError`` and is abandoned (the pool is shut down without
    waiting, mirroring the enrichment orchestrator).
    """
    results: Dict[str, Any] = {}
    if not jobs:
        return results
    pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(max_workers, len(jobs))),
        thread_name_prefix="pubdata",
    )
    try:
        futures = {pool.submit(fn): name for name, fn in jobs}
        remaining = max(0.0, deadline - time.monotonic())
        done, pending = concurrent.futures.wait(futures, timeout=remaining)
        for fut in done:
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as exc:  # noqa: BLE001 - classified by the caller
                results[name] = exc
        for fut in pending:
            fut.cancel()
            results[futures[fut]] = TimeoutError("source budget exhausted")
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return results


def _remaining(deadline: float, cap: float = CALL_TIMEOUT_S) -> float:
    """Per-call timeout: the call cap, never beyond the source deadline."""
    return max(0.5, min(cap, deadline - time.monotonic()))


# ---------------------------------------------------------------------------
# Data USA Tesseract (ACS 5-year, keyless)
# ---------------------------------------------------------------------------

TESSERACT_BASE = "https://api.datausa.io/tesseract"
CUBE_POPULATION = "acs_yg_total_population_5"  # ACS B01003
MEASURE_POPULATION = "Population"
CUBE_INCOME = "acs_ygr_median_household_income_race_5"  # ACS B19013
MEASURE_INCOME = "Household Income by Race"
_INCOME_RACE_TOTAL_ID = "0"  # Race member "Total" == the all-households median
_YEAR_TTL_S = 6 * 3600.0

#: Geography level -> (Tesseract level name, id prefix)
_LEVELS = {"Place": "16000US", "County": "05000US", "State": "04000US"}


def _tesseract_url(path: str, params: Dict[str, str]) -> str:
    query = urllib.parse.urlencode(
        params, quote_via=urllib.parse.quote, safe=":,;"
    )
    return f"{TESSERACT_BASE}/{path}?{query}"


def tesseract_members(
    cube: str, level: str, search: Optional[str] = None, timeout: float = CALL_TIMEOUT_S
) -> List[Dict[str, Any]]:
    """Members of one level ([{key, caption?}]). Raises FetchError."""
    params = {"cube": cube, "level": level}
    if search:
        params["search"] = search
    payload = request_json(_tesseract_url("members", params), timeout=timeout)
    members = payload.get("members") if isinstance(payload, dict) else None
    if not isinstance(members, list):
        raise FetchError("schema", "tesseract members response has no 'members' list")
    return [m for m in members if isinstance(m, dict)]


def acs_latest_year(timeout: float = CALL_TIMEOUT_S) -> int:
    """Newest ACS 5-year vintage published on Data USA (cached per process).

    Replaces the hard-coded ["2023", "2022", "2021"] list, which never tried
    the newest vintage (2024 is published; 2025 is not).
    """
    cached = _cache_get("acs_latest_year", _YEAR_TTL_S)
    if isinstance(cached, int):
        return cached
    members = tesseract_members(CUBE_POPULATION, "Year", timeout=timeout)
    years: List[int] = []
    for m in members:
        try:
            years.append(int(m.get("key")))
        except (TypeError, ValueError):
            continue
    if not years:
        raise FetchError("schema", "tesseract Year level returned no usable years")
    latest = max(years)
    _cache_put("acs_latest_year", latest)
    return latest


def tesseract_records(
    cube: str,
    level: str,
    measure: str,
    ids: Sequence[str],
    year: int,
    *,
    race_total: bool = False,
    timeout: float = CALL_TIMEOUT_S,
) -> Dict[str, Optional[float]]:
    """{geo_id: value} for ``ids`` at ``year`` (value None when suppressed)."""
    include = f"{level}:{','.join(ids)};Year:{year}"
    if race_total:
        include += f";Race:{_INCOME_RACE_TOTAL_ID}"
    url = _tesseract_url(
        "data.jsonrecords",
        {
            "cube": cube,
            "drilldowns": level,
            "measures": measure,
            "include": include,
        },
    )
    payload = request_json(url, timeout=timeout)
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise FetchError("schema", "tesseract data response has no 'data' list")
    out: Dict[str, Optional[float]] = {}
    id_key = f"{level} ID"
    for row in rows:
        if not isinstance(row, dict) or id_key not in row:
            continue
        value = row.get(measure)
        out[str(row[id_key])] = (
            float(value) if isinstance(value, (int, float)) and value >= 0 else None
        )
    return out


# -- place name matching ----------------------------------------------------

_PARENS_RE = re.compile(r"\s*\([^)]*\)\s*$")
_PARENS_INNER_RE = re.compile(r"\(([^)]*)\)\s*$")


def _norm_name(text: str) -> str:
    """Lowercase, accent-fold, drop punctuation: 'St. Louis' == 'st louis'."""
    folded = unicodedata.normalize("NFKD", text or "")
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    folded = folded.lower().replace("&", " and ")
    folded = re.sub(r"[^a-z0-9]+", " ", folded)
    return re.sub(r"\s+", " ", folded).strip()


# What a Census LEGAL name may add to the everyday place name. Derived from the
# ACS place-name conventions recorded in tests/fixtures/public_data_sources
# ("Indianapolis city (balance)", "Louisville/Jefferson County metro government
# (balance)", "Nashville-Davidson metropolitan government (balance)",
# "Athens-Clarke County unified government (balance)", "Augusta-Richmond County
# consolidated government (balance)", "Anchorage municipality", "<x> city and
# borough") plus the plain legal forms of the place_type column of the Census
# gazetteer. Anything else after the name ("Wayne HEIGHTS", "Wayne PARK",
# "Waynesboro") is a DIFFERENT place.
_LEGAL_NAME_SUFFIX_RE = re.compile(
    r"^(?:"
    r"city|town|village|borough|township|cdp|municipality|plantation|city and borough"
    r"|(?:[a-z0-9]+ ){0,2}(?:county )?(?:metro|metropolitan|unified|consolidated) government"
    r")$"
)


def is_legal_name_suffix(leftover: str) -> bool:
    """True when ``leftover`` (normalised words after the everyday place name)
    is only a legal-form suffix of that SAME place."""
    return bool(_LEGAL_NAME_SUFFIX_RE.match(leftover or ""))


def _county_qualifier(raw_base: str) -> str:
    """Normalised county qualifier of a same-name ACS place, or "".

    ACS disambiguates places that share a name inside one state by appending
    their county: "Burbank (Santa Clara County), CA" next to plain "Burbank, CA".
    "(balance)" is a legal-form note, not a county."""
    match = _PARENS_INNER_RE.search(raw_base)
    inner = _norm_name(match.group(1)) if match else ""
    return "" if inner in ("", "balance") else inner


def match_place_member(
    city: str,
    state_usps: str,
    members: Iterable[Dict[str, Any]],
    county_name: str = "",
) -> Optional[Tuple[str, str]]:
    """Pick the ACS place for "City, ST" from a name-search result -- or None.

    Never returns a different place that merely shares the first letters of the
    name ("Wayne, PA" must not become "Wayne Heights, PA", a place in another
    county 180 km away):

    * an EXACT name match (accents/punctuation folded, "(balance)" ignored) wins;
    * otherwise ONE candidate "<city> ..." is accepted only when the extra text
      is a legal-form suffix of the same place (``is_legal_name_suffix``);
    * a candidate carrying a county qualifier ("Burbank (Santa Clara County)") is
      accepted only when that county IS the target's (``county_name``); the
      unqualified sibling is then the target's only when no qualifier names the
      target's county;
    * ambiguity returns None -- the caller falls back to the (labelled) county,
      never to a guess.
    """
    suffix = f", {state_usps.upper()}"
    target = _norm_name(city)
    county = _norm_name(county_name)
    if not target:
        return None
    exact: List[Tuple[str, str]] = []
    prefixed: List[Tuple[str, str]] = []
    county_exact: List[Tuple[str, str]] = []  # explicitly the target's county
    for member in members:
        caption = str(member.get("caption") or "")
        key = str(member.get("key") or "")
        if not key or not caption.endswith(suffix):
            continue
        raw_base = caption[: -len(suffix)]
        base = _norm_name(_PARENS_RE.sub("", raw_base))
        qualifier = _county_qualifier(raw_base)
        if qualifier and qualifier != county:
            continue  # a same-name place in ANOTHER county (or unverifiable)
        if base == target:
            (county_exact if qualifier else exact).append((key, caption))
        elif base.startswith(target + " ") and is_legal_name_suffix(
            base[len(target) + 1 :]
        ):
            prefixed.append((key, caption))
    if len(county_exact) == 1:
        return county_exact[0]
    if len(exact) == 1 and not county_exact:
        return exact[0]
    if not exact and not county_exact and len(prefixed) == 1:
        return prefixed[0]
    return None


def resolve_place_id(
    city: str,
    state_usps: str,
    timeout: float = CALL_TIMEOUT_S,
    county_name: str = "",
) -> Optional[Tuple[str, str]]:
    """(place_id, caption) for the city, or None when ACS has no such place."""
    cache_key = (
        f"place::{_norm_name(city)}::{state_usps.upper()}::{_norm_name(county_name)}"
    )
    cached = _cache_get(cache_key, 30 * 24 * 3600.0)
    if cached is not None:
        return tuple(cached) if cached else None  # type: ignore[return-value]
    members = tesseract_members(CUBE_POPULATION, "Place", search=city, timeout=timeout)
    match = match_place_member(city, state_usps, members, county_name)
    # Cache hits AND clean misses (an absent place stays absent for a vintage).
    _cache_put(cache_key, list(match) if match else [])
    return match


# -- US geography targets & the tiered fetch --------------------------------


@dataclass(frozen=True)
class UsGeoTarget:
    """One US location to look up, already resolved to FIPS by the caller."""

    location: str  # the plan's own string -- the key in the result
    kind: str  # "city" | "county" | "state"
    city: str = ""  # everyday place name ("Hershey")
    state_usps: str = ""
    state_name: str = ""
    state_fips: str = ""  # 2 digits
    county_fips: str = ""  # 5 digits
    county_name: str = ""


@dataclass
class DemographicsResult:
    """Outcome of one ``fetch_us_demographics`` call."""

    entries: Dict[str, Dict[str, Any]]
    unresolved: Dict[str, Dict[str, Any]]
    year: Optional[int] = None


_ACS_SOURCE = "US Census ACS 5-year (via Data USA)"


def _vintage_label(year: int) -> str:
    return f"ACS {year - 4}-{year} 5-year"


def _tier_pair(
    level: str, ids: List[str], year: int, deadline: float
) -> Tuple[Callable[[], Any], Callable[[], Any]]:
    return (
        lambda: tesseract_records(
            CUBE_POPULATION,
            level,
            MEASURE_POPULATION,
            ids,
            year,
            timeout=_remaining(deadline),
        ),
        lambda: tesseract_records(
            CUBE_INCOME,
            level,
            MEASURE_INCOME,
            ids,
            year,
            race_total=True,
            timeout=_remaining(deadline),
        ),
    )


def _as_int(value: Optional[float]) -> Optional[int]:
    return int(round(value)) if isinstance(value, (int, float)) else None


def fetch_us_demographics(
    targets: Sequence[UsGeoTarget],
    *,
    budget_s: float = DEMOGRAPHICS_BUDGET_S,
    census_key: str = "",
) -> DemographicsResult:
    """Population + median household income for each US target.

    Tiering per target (each fallback is LABELLED on the entry, never passed
    off as the city's own number):

    1. ``Place``  -- the ACS place for the city (parallel name searches, then
       two batched queries for every resolved place).
    2. ``County`` -- the county of the city (FIPS from the local gazetteer).
    3. ``State``  -- the state; when Data USA is unreachable (or has no row)
       and ``census_key`` is set, the Census Bureau API itself is tried.

    Targets that are themselves a county/state ask that tier directly and the
    figure is the location's own (``geo_matches_location`` True).

    Never raises for a data problem: per-location failures come back in
    ``unresolved`` with a reason. The caller decides whether "nothing
    resolved" is a source failure.
    """
    deadline = time.monotonic() + max(1.0, budget_s)
    entries: Dict[str, Dict[str, Any]] = {}
    unresolved: Dict[str, Dict[str, Any]] = {}
    errors: List[str] = []  # transport failure kinds seen anywhere
    if not targets:
        return DemographicsResult(entries, unresolved)

    # Newest published vintage first (one cached call), not 3 serial 10 s tries.
    year: Optional[int] = None
    try:
        year = acs_latest_year(timeout=_remaining(deadline))
    except FetchError as exc:
        errors.append(f"data_usa_unreachable:{exc.kind}")

    # -- tier 1: places ------------------------------------------------------
    place_of: Dict[str, Tuple[str, str]] = {}
    search_errors: Dict[str, str] = {}
    place_pop: Dict[str, Optional[float]] = {}
    place_inc: Dict[str, Optional[float]] = {}
    city_targets = [t for t in targets if t.kind == "city" and t.city and t.state_usps]
    if year and city_targets:
        found = run_parallel(
            [
                (
                    t.location,
                    lambda t=t: resolve_place_id(
                        t.city,
                        t.state_usps,
                        timeout=_remaining(deadline),
                        county_name=t.county_name,
                    ),
                )
                for t in city_targets
            ],
            max_workers=5,
            deadline=deadline,
        )
        for target in city_targets:
            outcome = found.get(target.location)
            if isinstance(outcome, tuple):
                place_of[target.location] = outcome
            elif isinstance(outcome, BaseException):
                kind = outcome.kind if isinstance(outcome, FetchError) else "error"
                search_errors[target.location] = kind
                errors.append(f"place_search:{kind}")
        place_ids = [pid for pid, _cap in place_of.values()]
        if place_ids and time.monotonic() < deadline:
            pop_fn, inc_fn = _tier_pair("Place", place_ids, year, deadline)
            out = run_parallel(
                [("pop", pop_fn), ("inc", inc_fn)], max_workers=2, deadline=deadline
            )
            if isinstance(out.get("pop"), dict):
                place_pop = out["pop"]
            elif isinstance(out.get("pop"), BaseException):
                errors.append(f"place_data:{getattr(out['pop'], 'kind', 'error')}")
            if isinstance(out.get("inc"), dict):
                place_inc = out["inc"]

    remaining: List[UsGeoTarget] = []
    for target in targets:
        pid = place_of.get(target.location, ("", ""))[0]
        pop_v, inc_v = place_pop.get(pid), place_inc.get(pid)
        if year and pid and (pop_v is not None or inc_v is not None):
            entries[target.location] = {
                "population": _as_int(pop_v),
                "median_income": _as_int(inc_v),
                "state_name": target.state_name,
                "city": target.city,
                "source": _ACS_SOURCE,
                "geo_level": "Place",
                "geo_name": place_of[target.location][1],
                "geo_id": pid,
                "geo_matches_location": True,
                "year": year,
                "vintage": _vintage_label(year),
            }
        else:
            remaining.append(target)

    # -- tiers 2/3: county + state for everything not resolved at place level
    county_ids = sorted(
        {
            f"{_LEVELS['County']}{t.county_fips}"
            for t in remaining
            if t.county_fips and t.kind in ("city", "county")
        }
    )
    state_ids = sorted(
        {f"{_LEVELS['State']}{t.state_fips}" for t in remaining if t.state_fips}
    )
    tier: Dict[str, Dict[str, Optional[float]]] = {}
    if year and (county_ids or state_ids) and time.monotonic() < deadline:
        jobs: List[Tuple[str, Callable[[], Any]]] = []
        if county_ids:
            c_pop, c_inc = _tier_pair("County", county_ids, year, deadline)
            jobs += [("county_pop", c_pop), ("county_inc", c_inc)]
        if state_ids:
            s_pop, s_inc = _tier_pair("State", state_ids, year, deadline)
            jobs += [("state_pop", s_pop), ("state_inc", s_inc)]
        for name, value in run_parallel(jobs, max_workers=4, deadline=deadline).items():
            if isinstance(value, dict):
                tier[name] = value
            elif isinstance(value, BaseException):
                errors.append(f"{name}:{getattr(value, 'kind', 'error')}")

    census_memo: Dict[str, Dict[str, Dict[str, Any]]] = {}
    census_problem: List[str] = []

    def _census_states() -> Dict[str, Dict[str, Any]]:
        """Direct Census state rows, fetched at most once, only with a key."""
        if "v" not in census_memo:
            census_memo["v"] = {}
            if not census_key:
                census_problem.append("census_key_not_configured")
            elif time.monotonic() < deadline:
                try:
                    census_memo["v"] = fetch_census_states(
                        census_key, timeout=_remaining(deadline)
                    )
                except FetchError as exc:
                    census_problem.append(census_key_problem(exc) or f"census:{exc.kind}")
        return census_memo["v"]

    for target in remaining:
        tried: List[str] = ["place"] if target.kind == "city" else []
        if target.kind == "city":
            if target.location in search_errors:
                why = f"place_search_failed:{search_errors[target.location]}"
            elif not year:
                why = "data_usa_unreachable"
            else:
                why = "place_not_in_acs"
        else:
            why = ""
        placed = False
        if year and target.county_fips and target.kind in ("city", "county"):
            cid = f"{_LEVELS['County']}{target.county_fips}"
            tried.append("county")
            c_pop = tier.get("county_pop", {}).get(cid)
            c_inc = tier.get("county_inc", {}).get(cid)
            if c_pop is not None or c_inc is not None:
                label = f"{target.county_name}, {target.state_usps}".strip(", ")
                entries[target.location] = _area_entry(
                    target, "County", cid, c_pop, c_inc, year, name=label, why=why
                )
                placed = True
        if not placed and target.state_fips:
            sid = f"{_LEVELS['State']}{target.state_fips}"
            tried.append("state")
            s_pop = tier.get("state_pop", {}).get(sid) if year else None
            s_inc = tier.get("state_inc", {}).get(sid) if year else None
            source, entry_year = _ACS_SOURCE, year
            if s_pop is None and s_inc is None:
                row = _census_states().get(target.state_fips)
                if row:
                    tried.append("census_api")
                    s_pop, s_inc = row.get("population"), row.get("median_income")
                    source = "US Census ACS 5-year (api.census.gov)"
                    entry_year = _CENSUS_STATE.get("year") or year
            if (s_pop is not None or s_inc is not None) and entry_year:
                entry = _area_entry(
                    target,
                    "State",
                    sid,
                    s_pop,
                    s_inc,
                    entry_year,
                    name=target.state_name or target.state_usps,
                    why=why,
                )
                entry["source"] = source
                entries[target.location] = entry
                placed = True
        if not placed:
            reason = why or "geography_not_in_acs"
            if not year and census_problem:
                reason = f"{reason};{census_problem[0]}"
            unresolved[target.location] = {
                "reason": reason,
                "tried": tried,
                "city": target.city,
                "state": target.state_usps,
            }

    return DemographicsResult(entries, unresolved, year)


def _area_entry(
    target: UsGeoTarget,
    level: str,
    geo_id: str,
    pop: Optional[float],
    income: Optional[float],
    year: int,
    *,
    name: str,
    why: str,
) -> Dict[str, Any]:
    """Entry for a county/state figure.

    When the target IS that county/state the figure is its own
    (``population`` populated). When it is a fallback for a CITY the figure is
    published as ``area_population`` / ``area_median_income`` ONLY, so no
    consumer can mistake it for the city's number.
    """
    matches = (level == "County" and target.kind == "county") or (
        level == "State" and target.kind == "state"
    )
    entry: Dict[str, Any] = {
        "state_name": target.state_name,
        "city": target.city,
        "source": _ACS_SOURCE,
        "geo_level": level,
        "geo_name": name,
        "geo_id": geo_id,
        "geo_matches_location": matches,
        "year": year,
        "vintage": _vintage_label(year),
    }
    if matches:
        entry["population"] = _as_int(pop)
        entry["median_income"] = _as_int(income)
    else:
        entry["area_population"] = _as_int(pop)
        entry["area_median_income"] = _as_int(income)
        entry["fallback_reason"] = why or "city_level_unavailable"
    return entry


# ---------------------------------------------------------------------------
# Census Bureau API (needs a key on EVERY query since 2026)
# ---------------------------------------------------------------------------

_CENSUS_ACS_URL = "https://api.census.gov/data/{year}/acs/acs5"
_CENSUS_VINTAGES = (2024, 2023)  # newest first; 2025 is not published (404)
_CENSUS_STATE: Dict[str, Any] = {}
_CENSUS_STATE_LOCK = threading.Lock()


def census_key_problem(exc: FetchError) -> str:
    """'' unless the failure is the Census key gate; then a stable reason."""
    if exc.kind == "redirect" and "missing_key" in exc.location:
        return "census_key_missing"
    if exc.kind == "redirect" and "invalid_key" in exc.location:
        return "census_key_invalid"
    return ""


def fetch_census_states(key: str, timeout: float = CALL_TIMEOUT_S) -> Dict[str, Dict[str, Any]]:
    """{state_fips: {name, population, median_income}} straight from Census.

    Newest vintage first; falls back to the previous one ONLY on a real
    "not published" 404, and remembers which vintage worked for the life of
    the process. A key rejection is remembered too, so a bad/absent key costs
    one 1-second round-trip per process instead of one per plan.
    """
    if not key:
        raise FetchError("redirect", "no CENSUS_API_KEY", location="missing_key")
    with _CENSUS_STATE_LOCK:
        if _CENSUS_STATE.get("key_rejected") == key:
            raise FetchError("redirect", "key previously rejected", location="invalid_key")
        known_year = _CENSUS_STATE.get("year")
    years = [known_year] if known_year else list(_CENSUS_VINTAGES)
    last: Optional[FetchError] = None
    for year in years:
        url = (
            _CENSUS_ACS_URL.format(year=year)
            + "?"
            + urllib.parse.urlencode(
                {"get": "NAME,B01003_001E,B19013_001E", "for": "state:*", "key": key},
                safe=":*,",
            )
        )
        try:
            payload = request_json(url, timeout=timeout)
        except FetchError as exc:
            last = exc
            if census_key_problem(exc):
                with _CENSUS_STATE_LOCK:
                    _CENSUS_STATE["key_rejected"] = key
                raise
            if exc.kind == "http" and exc.status == 404:
                continue  # vintage not published yet -> try the previous one
            raise
        rows = _parse_census_states(payload)
        with _CENSUS_STATE_LOCK:
            _CENSUS_STATE["year"] = year
        return rows
    raise last or FetchError("http", "no ACS vintage published", 404)


def _parse_census_states(payload: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[0], list):
        raise FetchError("schema", "census ACS payload is not [header,rows...]")
    header = payload[0]
    try:
        i_name, i_pop, i_inc, i_fips = (
            header.index("NAME"),
            header.index("B01003_001E"),
            header.index("B19013_001E"),
            header.index("state"),
        )
    except ValueError as exc:
        raise FetchError("schema", f"census ACS header missing a column: {exc}") from exc
    out: Dict[str, Dict[str, Any]] = {}
    for row in payload[1:]:
        if not isinstance(row, list) or len(row) <= max(i_name, i_pop, i_inc, i_fips):
            continue
        try:
            out[str(row[i_fips])] = {
                "name": row[i_name],
                "population": int(row[i_pop]) if row[i_pop] not in (None, "") else None,
                "median_income": int(row[i_inc]) if row[i_inc] not in (None, "") else None,
            }
        except (TypeError, ValueError):
            continue
    if not out:
        raise FetchError("schema", "census ACS payload had no parsable state rows")
    return out


# ---------------------------------------------------------------------------
# World Bank country population (non-US locations: COUNTRY level only)
# ---------------------------------------------------------------------------


def fetch_country_population(iso3: str, timeout: float = CALL_TIMEOUT_S) -> Optional[int]:
    """Latest World Bank total population for a country (None: no value)."""
    url = (
        f"https://api.worldbank.org/v2/country/{urllib.parse.quote(iso3)}"
        "/indicator/SP.POP.TOTL?format=json&per_page=5&date=2019:2024"
    )
    payload = request_json(url, timeout=timeout)
    if not (isinstance(payload, list) and len(payload) >= 2):
        raise FetchError("schema", "world bank response is not [meta, records]")
    for rec in payload[1] or []:
        if isinstance(rec, dict) and isinstance(rec.get("value"), (int, float)):
            return int(rec["value"])
    return None


# ---------------------------------------------------------------------------
# ILO ILOSTAT SDMX (annual labour indicators)
# ---------------------------------------------------------------------------

ILO_BASE = "https://sdmx.ilo.org/rest/data"
#: indicator -> (dataflow, key template). One SERIES per request: a multi-member
#: key makes the server emit duplicate JSON object keys (silent data loss).
ILO_SERIES: Dict[str, Tuple[str, str]] = {
    "unemployment_rate": (
        "ILO,DF_UNE_DEAP_SEX_AGE_RT",
        "{c}.A.UNE_DEAP_RT.SEX_T.AGE_YTHADULT_YGE15",
    ),
    "youth_unemployment": (
        "ILO,DF_UNE_DEAP_SEX_AGE_RT",
        "{c}.A.UNE_DEAP_RT.SEX_T.AGE_YTHADULT_Y15-24",
    ),
    "labor_force_participation": (
        "ILO,DF_EAP_DWAP_SEX_AGE_RT",
        "{c}.A.EAP_DWAP_RT.SEX_T.AGE_YTHADULT_YGE15",
    ),
}


def parse_ilo_latest(payload: Any) -> Tuple[float, str]:
    """(value, period) of the newest numeric observation (SDMX-JSON 2.0)."""
    try:
        data = payload["data"]
        periods = data["structures"][0]["dimensions"]["observation"][0]["values"]
        series = data["dataSets"][0]["series"]
    except (KeyError, IndexError, TypeError) as exc:
        raise FetchError("schema", f"ILO SDMX payload shape: {exc!r}") from exc
    best: Optional[Tuple[str, float]] = None
    for sdata in (series or {}).values():
        for idx, obs in (sdata.get("observations") or {}).items():
            try:
                period = str(periods[int(idx)]["id"])
                value = float(obs[0])
            except (IndexError, KeyError, TypeError, ValueError):
                continue
            if best is None or period > best[0]:
                best = (period, value)
    if best is None:
        raise FetchError("schema", "ILO SDMX payload had no numeric observation")
    return best[1], best[0]


def fetch_ilo_indicator(
    iso3: str, indicator: str, timeout: float = CALL_TIMEOUT_S
) -> Tuple[float, str]:
    flow, key = ILO_SERIES[indicator]
    url = (
        f"{ILO_BASE}/{flow}/{key.format(c=iso3)}"
        f"?format=jsondata&startPeriod={time.gmtime().tm_year - 4}"
    )
    return parse_ilo_latest(
        request_json(
            url, headers={"Accept": "application/vnd.sdmx.data+json"}, timeout=timeout
        )
    )


def fetch_ilo_labour(
    iso3_codes: Sequence[str], *, budget_s: float = ILO_BUDGET_S, max_countries: int = 6
) -> Dict[str, Any]:
    """Live ILOSTAT labour indicators per country.

    Returns ``{}`` when there is nothing to fetch (no codes). Raises
    ``SourceFailure`` when countries were requested and NONE returned data.
    """
    codes: List[str] = []
    for code in iso3_codes:
        if code and code not in codes:
            codes.append(code)
    codes = codes[:max_countries]
    if not codes:
        return {}
    deadline = time.monotonic() + max(1.0, budget_s)
    jobs = [
        (
            f"{code}:{indicator}",
            lambda c=code, i=indicator: fetch_ilo_indicator(
                c, i, timeout=_remaining(deadline)
            ),
        )
        for code in codes
        for indicator in ILO_SERIES
    ]
    outcomes = run_parallel(jobs, max_workers=4, deadline=deadline)
    countries: Dict[str, Dict[str, Any]] = {}
    failures: Dict[str, str] = {}
    for code in codes:
        row: Dict[str, Any] = {}
        for indicator in ILO_SERIES:
            outcome = outcomes.get(f"{code}:{indicator}")
            if isinstance(outcome, tuple):
                value, period = outcome
                row[indicator] = round(value, 1)
                row["year"] = max(str(row.get("year") or ""), period)
            elif isinstance(outcome, BaseException):
                failures[f"{code}:{indicator}"] = getattr(outcome, "kind", type(outcome).__name__)
        if "unemployment_rate" in row:
            row.update({"source": "ILO ILOSTAT SDMX (live)", "data_confidence": 0.90})
            countries[code] = row
    if not countries:
        raise SourceFailure(
            f"ILOSTAT returned no data for {codes}: {sorted(set(failures.values())) or 'no response'}"
        )
    result: Dict[str, Any] = {
        "source": "ILO ILOSTAT",
        "countries": countries,
        "data_confidence": 0.90,
    }
    missing = [c for c in codes if c not in countries]
    if missing:
        result["failed_countries"] = missing
    return result


# ---------------------------------------------------------------------------
# Statistics Canada Web Data Service
# ---------------------------------------------------------------------------

STATCAN_WDS_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods"
#: Labour Force Survey, table 14-10-0287-01: Canada, unemployment rate,
#: both sexes, 15+, seasonally adjusted (series title verified live 2026-10-01).
STATCAN_UNEMPLOYMENT_VECTOR = 2062815
STATCAN_UNEMPLOYMENT_SERIES = (
    "Canada; Unemployment rate; Total - Gender; 15 years and over; "
    "Estimate; Seasonally adjusted (LFS, table 14-10-0287-01)"
)


def fetch_statcan_vector(
    vector_id: int = STATCAN_UNEMPLOYMENT_VECTOR,
    latest_n: int = 6,
    timeout: float = CALL_TIMEOUT_S,
) -> Dict[str, Any]:
    """Latest observations of one StatCan vector. Raises FetchError."""
    payload = request_json(
        STATCAN_WDS_URL,
        method="POST",
        body=[{"vectorId": int(vector_id), "latestN": int(latest_n)}],
        timeout=timeout,
    )
    try:
        item = payload[0]
        if item.get("status") != "SUCCESS":
            raise FetchError("schema", f"StatCan WDS status {item.get('status')!r}")
        points = item["object"]["vectorDataPoint"]
        recent = [
            {"period": p["refPer"], "value": float(p["value"])}
            for p in points
            if p.get("value") is not None
        ]
    except (IndexError, KeyError, TypeError, ValueError, AttributeError) as exc:
        raise FetchError("schema", f"StatCan WDS payload shape: {exc!r}") from exc
    if not recent:
        raise FetchError("schema", "StatCan WDS returned no data points")
    return {
        "vector_id": int(vector_id),
        "recent": recent,
        "latest_value": recent[-1]["value"],
        "latest_period": recent[-1]["period"],
    }
