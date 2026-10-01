"""Regression tests: the Supabase upsert in data_enrichment.py.

Prod telemetry 2026-10-01 (ledger A_prod_telemetry section 2 row 2): every
enrichment cycle logged ``_do_upsert_batch: HTTP Error 400: Bad Request`` --
1,519 warnings + 521 "All 3 retries exhausted" per week -- and the persisted
state never updated, so a refresh "never stuck".

Root cause (verified against the repo's schema files):

1. ``_mark_refreshed`` wrote to ``enrichment_log`` with
   ``?on_conflict=source,action`` + ``Prefer: resolution=merge-duplicates``.
   ``enrichment_log`` is an append-only audit table keyed by a serial ``id``
   (scripts/supabase_schema.sql, s37_missing_table_schemas.sql) with NO
   UNIQUE(source, action), so Postgres answered 42P10 -> HTTP 400 on every
   single refresh.
2. ``_retry_with_backoff`` caught ``HTTPError`` (a URLError subclass) and
   retried the deterministic 400 three times (2 + 4 + 8 s of backoff) without
   ever reading the response body -- the ``except HTTPError`` branch in
   ``_upsert_to_supabase`` (body logging + the 401 circuit breaker) was
   unreachable.
3. The ``salary_data`` payload used columns that exist in no schema file
   (salary_range_low/high, source, scraped_at) and ``on_conflict=role,location``
   instead of the table's UNIQUE(role, location, industry).
"""

from __future__ import annotations

import io
import json
import logging
import re
import urllib.error
from email.message import Message
from pathlib import Path
from typing import Any, Callable

import pytest

import data_enrichment as de

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASE = "https://example.supabase.co"


def _http_error(code: int, body: bytes = b"") -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url=f"{BASE}/rest/v1/x",
        code=code,
        msg="err",
        hdrs=Message(),
        fp=io.BytesIO(body),
    )


class _OkResponse:
    def __enter__(self) -> "_OkResponse":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def getcode(self) -> int:
        return 201

    def read(self, *a: Any) -> bytes:
        return b""


class _Net:
    """Scriptable urlopen: ``outcome(req)`` returns an exception to raise or None."""

    def __init__(self, outcome: Callable[[Any], Exception | None]) -> None:
        self.outcome = outcome
        self.requests: list[Any] = []

    def __call__(self, req: Any, timeout: Any = None, context: Any = None) -> Any:
        self.requests.append(req)
        err = self.outcome(req)
        if err is not None:
            raise err
        return _OkResponse()


@pytest.fixture()
def sb(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Supabase 'configured', breakers reset, sleeps recorded, alerts recorded."""
    sleeps: list[float] = []
    alerts: list[tuple[str, str]] = []
    monkeypatch.setattr(de, "_SUPABASE_ENABLED", True)
    monkeypatch.setattr(de, "SUPABASE_URL", BASE)
    monkeypatch.setattr(de, "SUPABASE_KEY", "test-service-role-key")
    monkeypatch.setattr(de, "_supabase_global_fail_time", 0.0)
    monkeypatch.setattr(de, "_supabase_table_fail_times", {})
    monkeypatch.setattr(de.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(
        de,
        "send_alert",
        lambda subject, body, severity="info": alerts.append((subject, severity)),
    )
    return {"sleeps": sleeps, "alerts": alerts, "monkeypatch": monkeypatch}


def _install(sb: dict[str, Any], net: _Net) -> _Net:
    sb["monkeypatch"].setattr(de.urllib.request, "urlopen", net)
    return net


def _engine(monkeypatch: pytest.MonkeyPatch) -> de.DataEnrichmentEngine:
    """Engine constructed with Supabase off (no state load over the network)."""
    monkeypatch.setattr(de, "_SUPABASE_ENABLED", False)
    eng = de.DataEnrichmentEngine()
    monkeypatch.setattr(de, "_SUPABASE_ENABLED", True)
    return eng


# ---------------------------------------------------------------------------
# 1. Root cause: enrichment_log is append-only, not an upsert
# ---------------------------------------------------------------------------


def test_enrichment_log_write_is_a_plain_insert_not_an_on_conflict_upsert(
    sb: dict[str, Any],
) -> None:
    net = _install(sb, _Net(lambda req: None))
    eng = _engine(sb["monkeypatch"])
    eng._save_enrichment_log_to_supabase("bls_salary", True, 3)

    assert len(net.requests) == 1
    req = net.requests[0]
    assert req.full_url == f"{BASE}/rest/v1/enrichment_log", req.full_url
    assert "on_conflict" not in req.full_url
    prefer = req.get_header("Prefer") or ""
    assert "merge-duplicates" not in prefer
    rows = json.loads(req.data)
    assert len(rows) == 1
    assert rows[0]["source"] == "bls_salary"
    assert rows[0]["action"] == "refresh"
    assert rows[0]["table_name"] == "data_enrichment"
    assert json.loads(rows[0]["details"]) == {"success": True}


def test_enrichment_log_has_no_on_conflict_entry_and_matches_the_schema() -> None:
    """The authoritative DDL has no UNIQUE(source, action) on enrichment_log."""
    assert "enrichment_log" not in de._ON_CONFLICT_MAP
    ddl = (PROJECT_ROOT / "scripts" / "supabase_schema.sql").read_text(encoding="utf-8")
    block = re.search(
        r"CREATE TABLE IF NOT EXISTS enrichment_log \((.*?)\n\);", ddl, re.S
    )
    assert block, "enrichment_log DDL not found"
    assert "UNIQUE" not in block.group(1).upper()


def _table_ddl(table: str) -> tuple[set[str], list[str]]:
    ddl = (PROJECT_ROOT / "scripts" / "supabase_schema.sql").read_text(encoding="utf-8")
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\);", ddl, re.S)
    assert m, f"{table} DDL not found"
    cols: set[str] = set()
    unique: list[str] = []
    for line in m.group(1).splitlines():
        line = line.split("--")[0].strip().rstrip(",")
        if not line:
            continue
        if line.upper().startswith("UNIQUE"):
            unique = [c.strip() for c in re.search(r"\((.*)\)", line).group(1).split(",")]  # type: ignore[union-attr]
            continue
        cols.add(line.split()[0])
    return cols, unique


@pytest.mark.parametrize("table", ["knowledge_base", "salary_data"])
def test_on_conflict_columns_equal_the_tables_unique_constraint(table: str) -> None:
    _cols, unique = _table_ddl(table)
    assert unique, f"{table} must declare a UNIQUE constraint"
    assert de._ON_CONFLICT_MAP[table].split(",") == unique


def test_salary_rows_only_use_columns_that_exist_in_the_schema(
    sb: dict[str, Any], tmp_path: Path
) -> None:
    import api_enrichment

    mp = sb["monkeypatch"]
    mp.setattr(de, "ENRICHMENT_STATE_FILE", tmp_path / "state.json")
    mp.setattr(de, "_generate_llm_summary", lambda *a, **k: "")
    mp.setattr(
        api_enrichment,
        "fetch_salary_data",
        lambda roles: {
            "Registered Nurse": {
                "median": 82000,
                "p10": 59000,
                "p90": 120000,
                "source": "BLS OES",
            }
        },
    )
    net = _install(sb, _Net(lambda req: None))
    eng = _engine(mp)
    eng._enrich_bls_salary()

    salary_reqs = [
        r for r in net.requests if r.full_url.split("?")[0].endswith("/salary_data")
    ]
    assert len(salary_reqs) == 1
    req = salary_reqs[0]
    assert req.full_url.endswith("?on_conflict=role,location,industry")
    cols, unique = _table_ddl("salary_data")
    row = json.loads(req.data)[0]
    assert set(row) <= cols, f"unknown columns sent: {set(row) - cols}"
    assert row["salary_10th"] == 59000 and row["salary_90th"] == 120000
    assert row["median_salary"] == 82000 and row["data_source"] == "BLS"
    # every column of the conflict target is resolvable for the row
    for col in unique:
        assert col in row or col in cols


def test_knowledge_base_upsert_keeps_on_conflict_and_merge_duplicates(
    sb: dict[str, Any],
) -> None:
    net = _install(sb, _Net(lambda req: None))
    n = de._upsert_to_supabase(
        "knowledge_base", [{"category": "fred_economic", "key": "UNRATE", "data": {}}]
    )
    assert n == 1
    req = net.requests[0]
    assert req.full_url == f"{BASE}/rest/v1/knowledge_base?on_conflict=category,key"
    assert req.get_header("Prefer") == "resolution=merge-duplicates"


# ---------------------------------------------------------------------------
# 2. Permanent 4xx: not retried, body read + logged (redacted, truncated)
# ---------------------------------------------------------------------------

_PG_42P10 = (
    b'{"code":"42P10","details":null,"hint":null,"message":"there is no unique '
    b'or exclusion constraint matching the ON CONFLICT specification"}'
)


def test_permanent_400_is_not_retried_and_its_body_is_logged(
    sb: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    net = _install(sb, _Net(lambda req: _http_error(400, _PG_42P10)))
    with caplog.at_level(logging.DEBUG, logger="data_enrichment"):
        n = de._upsert_to_supabase(
            "knowledge_base", [{"category": "c", "key": "k", "data": {}}]
        )
    assert n == 0
    assert len(net.requests) == 1, "a deterministic 400 must not be retried"
    assert sb["sleeps"] == [], "no backoff sleep for a permanent error"
    text = " ".join(
        r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING
    )
    assert "42P10" in text
    assert "no unique or exclusion constraint" in text
    assert "on_conflict=category,key" in text
    assert "All 3 retries exhausted" not in text


def _kb_rows(batches: int) -> list[dict[str, Any]]:
    return [
        {"category": "c", "key": str(i), "data": {}}
        for i in range(de._BATCH_SIZE * batches)
    ]


@pytest.mark.parametrize("code", [403, 404])
def test_table_wide_client_errors_abandon_the_remaining_batches(
    sb: dict[str, Any], code: int
) -> None:
    """403 (forbidden) / 404 (no such table) hit every batch identically."""
    net = _install(sb, _Net(lambda req: _http_error(code, b"{}")))
    de._upsert_to_supabase("knowledge_base", _kb_rows(3))
    assert len(net.requests) == 1, "remaining chunks would fail identically"
    assert sb["sleeps"] == []


@pytest.mark.parametrize("code", [400, 409, 422])
def test_row_specific_errors_skip_only_that_batch_and_continue(
    sb: dict[str, Any], caplog: pytest.LogCaptureFixture, code: int
) -> None:
    """One bad row must not stop the rest of the upload: batch 2 of 3 is
    rejected, batches 1 and 3 are still sent and counted; nothing is retried."""
    state = {"n": 0}

    def outcome(req: Any) -> Exception | None:
        state["n"] += 1
        if state["n"] == 2:
            return _http_error(code, b'{"message":"bad row","api_key":"x"}')
        return None

    net = _install(sb, _Net(outcome))
    with caplog.at_level(logging.DEBUG, logger="data_enrichment"):
        stored = de._upsert_to_supabase("knowledge_base", _kb_rows(3))
    assert len(net.requests) == 3, "one request per batch, the failed one NOT retried"
    assert sb["sleeps"] == []
    assert stored == de._BATCH_SIZE * 2, "batches 1 and 3 were stored"
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1, errors
    assert "batch 2/3" in errors[0] and "bad row" in errors[0]
    assert str(code) in errors[0]


def test_row_specific_error_body_is_still_redacted(
    sb: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    body = b'{"message":"https://x.example/?api_key=FAKESECRETKEY9999&a=1"}'
    _install(sb, _Net(lambda req: _http_error(400, body)))
    with caplog.at_level(logging.DEBUG, logger="data_enrichment"):
        de._upsert_to_supabase("knowledge_base", _kb_rows(1))
    assert "FAKESECRETKEY9999" not in " ".join(r.getMessage() for r in caplog.records)


def test_a_401_still_abandons_the_remaining_batches(sb: dict[str, Any]) -> None:
    net = _install(sb, _Net(lambda req: _http_error(401, b"{}")))
    de._upsert_to_supabase("knowledge_base", _kb_rows(3))
    assert len(net.requests) == 1


def test_error_body_is_redacted_and_truncated_in_the_log(
    sb: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    body = (
        b'{"message":"failed for https://x.example/?api_key=FAKESECRETKEY9999&a=1"}'
        + b"x" * 5000
    )
    _install(sb, _Net(lambda req: _http_error(400, body)))
    with caplog.at_level(logging.DEBUG, logger="data_enrichment"):
        de._upsert_to_supabase(
            "knowledge_base", [{"category": "c", "key": "k", "data": {}}]
        )
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "FAKESECRETKEY9999" not in text
    err = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR][0]
    assert len(err) < 800, "body must be length-capped"


# ---------------------------------------------------------------------------
# 3. Transient errors are still retried, now with the body visible
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", [429, 500, 503])
def test_transient_errors_are_retried_with_backoff_and_body_logged(
    sb: dict[str, Any], caplog: pytest.LogCaptureFixture, code: int
) -> None:
    net = _install(sb, _Net(lambda req: _http_error(code, b"upstream connect error")))
    with caplog.at_level(logging.DEBUG, logger="data_enrichment"):
        n = de._upsert_to_supabase(
            "knowledge_base", [{"category": "c", "key": "k", "data": {}}]
        )
    assert n == 0
    assert len(net.requests) == 4, "1 attempt + 3 retries"
    assert len(sb["sleeps"]) == 3
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "upstream connect error" in text
    assert "All 3 retries exhausted" in text


def test_transient_failure_then_success_returns_the_rows(sb: dict[str, Any]) -> None:
    state = {"n": 0}

    def outcome(req: Any) -> Exception | None:
        state["n"] += 1
        return _http_error(503, b"busy") if state["n"] == 1 else None

    net = _install(sb, _Net(outcome))
    n = de._upsert_to_supabase(
        "knowledge_base", [{"category": "c", "key": "k", "data": {}}]
    )
    assert n == 1 and len(net.requests) == 2


# ---------------------------------------------------------------------------
# 4. The 401 circuit breaker (previously unreachable) now works
# ---------------------------------------------------------------------------


def test_401_trips_the_per_table_breaker_and_skips_the_next_call(
    sb: dict[str, Any],
) -> None:
    net = _install(
        sb, _Net(lambda req: _http_error(401, b'{"message":"Invalid API key"}'))
    )
    row = [{"category": "c", "key": "k", "data": {}}]
    assert de._upsert_to_supabase("knowledge_base", row) == 0
    assert len(net.requests) == 1, "401 is permanent: no retries"
    assert "knowledge_base" in de._supabase_table_fail_times
    assert [s for s, sev in sb["alerts"] if sev == "critical"], "critical alert sent"
    before = len(net.requests)
    assert de._upsert_to_supabase("knowledge_base", row) == 0
    assert len(net.requests) == before, "breaker open: no network call"


# ---------------------------------------------------------------------------
# 5. Helper unit tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code,transient",
    [
        (400, False),
        (401, False),
        (403, False),
        (404, False),
        (409, False),
        (422, False),
        (408, True),
        (425, True),
        (429, True),
        (500, True),
        (502, True),
        (503, True),
    ],
)
def test_is_transient_http(code: int, transient: bool) -> None:
    assert de._is_transient_http(code) is transient


def test_http_error_body_is_read_once_and_cached() -> None:
    err = _http_error(400, b"  line one\n\nline   two  ")
    assert de._http_error_body(err) == "line one line two"
    assert de._http_error_body(err) == "line one line two", "second read uses the cache"


def test_http_error_body_survives_an_unreadable_response() -> None:
    err = urllib.error.HTTPError(f"{BASE}/x", 400, "err", Message(), None)
    assert de._http_error_body(err) == ""
