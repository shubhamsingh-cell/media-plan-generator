"""Regression tests: a redeploy must not re-run (and burn the quota of) BLS & co.

Prod telemetry 2026-10-01 (ledger A_prod_telemetry section 2 row 4): 978
``BLS v1 failed ... daily threshold ... has been reached`` warnings, all between
Sep 24 17:00 and 20:00Z, the window with 33 deploys (978 / 33 = ~30 lines per
boot = one 10-role BLS pass per boot).

Verdict: YES, every boot re-ran BLS. Mechanism (all code-verified):

1. ``ENABLE_DATA_ENRICHMENT=1`` starts ``DataEnrichmentEngine`` at import; its
   first ``run_cycle`` fires 5 minutes after boot and refreshes every source
   whose persisted ``last_runs`` entry is missing/older than its threshold.
2. The persisted state lives in the Supabase ``enrichment_log`` table -- but
   every write to it returned HTTP 400 (``on_conflict=source,action`` without a
   matching UNIQUE constraint; see test_data_enrichment_supabase_upsert.py), so
   nothing was ever persisted. ``data/enrichment_state.json`` is gitignored and
   the Render filesystem is ephemeral, so after each deploy ``last_runs`` was
   empty and ALL sources looked never-refreshed.
3. Under gunicorn --preload + gevent the engine timer is inherited by every
   forked worker (wsgi.py documents this), so N+1 copies each refreshed at the
   same moment -- 5x the BLS requests on top of (2). BLS v1 (no registration
   key) allows 25 queries/day; one 10-role pass is 10.

Fixes pinned here: the persisted state now survives a redeploy (append-only
insert), is re-read at the start of every cycle and before each refresh, reads
a generous window of rows with tolerant timestamp parsing, and a per-source
cross-process claim makes sure that when several copies find the same source
stale only ONE of them fetches.
"""

from __future__ import annotations

import io
import json
import threading
import time
import urllib.error
import urllib.parse
from datetime import datetime, timedelta, timezone
from email.message import Message
from pathlib import Path
from typing import Any, Callable

import pytest

import data_enrichment as de

BASE = "https://example.supabase.co"


class _Resp:
    def __init__(self, status: int = 201, body: bytes = b"") -> None:
        self.status = status
        self._body = body

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def getcode(self) -> int:
        return self.status

    def read(self, *a: Any) -> bytes:
        return self._body


def _http_error(code: int, body: bytes) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(f"{BASE}/x", code, "err", Message(), io.BytesIO(body))


def _pg_timestamp() -> str:
    """A PostgREST-style timestamptz: 5 fractional digits (trailing zero trimmed),
    which Python 3.9's datetime.fromisoformat() cannot parse."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-1] + "+00:00"


class _FakePostgrest:
    """Just enough PostgREST for enrichment_log, enforcing prod's constraints:
    serial id primary key, NO UNIQUE(source, action)."""

    def __init__(self) -> None:
        self.log_rows: list[dict[str, Any]] = []
        self.requests: list[Any] = []
        self._lock = threading.Lock()

    def __call__(self, req: Any, timeout: Any = None, context: Any = None) -> Any:
        self.requests.append(req)
        parsed = urllib.parse.urlparse(req.full_url)
        table = parsed.path.rsplit("/", 1)[-1]
        qs = urllib.parse.parse_qs(parsed.query)
        if table != "enrichment_log":
            return _Resp(201)  # knowledge_base / salary_data: accept
        if req.get_method() == "POST":
            if "on_conflict" in qs:
                raise _http_error(
                    400,
                    b'{"code":"42P10","message":"there is no unique or exclusion '
                    b'constraint matching the ON CONFLICT specification"}',
                )
            rows = json.loads(req.data)
            with self._lock:
                for row in rows:
                    stored = dict(row)
                    stored["id"] = len(self.log_rows) + 1
                    stored["created_at"] = _pg_timestamp()
                    self.log_rows.append(stored)
            return _Resp(201)
        # GET
        rows = list(self.log_rows)
        if "table_name" in qs:
            want = qs["table_name"][0].removeprefix("eq.")
            rows = [r for r in rows if r.get("table_name") == want]
        rows.sort(key=lambda r: r["created_at"], reverse=True)
        limit = int((qs.get("limit") or ["1000"])[0])
        body = json.dumps(
            [
                {
                    "source": r["source"],
                    "records_affected": r["records_affected"],
                    "details": r["details"],
                    "created_at": r["created_at"],
                }
                for r in rows[:limit]
            ]
        ).encode()
        return _Resp(200, body)


@pytest.fixture()
def world(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, Any]:
    """A fake Supabase + an api_enrichment whose BLS fetch is counted."""
    import api_enrichment

    pg = _FakePostgrest()
    counts = {"bls": 0}
    bls_delay = {"s": 0.0}
    bls_result: dict[str, Any] = {
        "data": {"Registered Nurse": {"median": 82000, "p10": 59000, "p90": 120000}}
    }

    def fake_fetch_salary_data(roles: list[str]) -> dict[str, Any]:
        counts["bls"] += 1
        time.sleep(bls_delay["s"])
        return dict(bls_result["data"])

    monkeypatch.setenv("NOVA_SLOT_DIR", str(tmp_path / "slots"))
    monkeypatch.setattr(de, "_SUPABASE_ENABLED", True)
    monkeypatch.setattr(de, "SUPABASE_URL", BASE)
    monkeypatch.setattr(de, "SUPABASE_KEY", "test-service-role-key")
    monkeypatch.setattr(de, "ENRICHMENT_STATE_FILE", tmp_path / "no-such-state.json")
    monkeypatch.setattr(de, "_supabase_table_fail_times", {})
    monkeypatch.setattr(de, "_supabase_global_fail_time", 0.0)
    monkeypatch.setattr(de.urllib.request, "urlopen", pg)
    monkeypatch.setattr(de, "_generate_llm_summary", lambda *a, **k: "")
    monkeypatch.setattr(de, "send_alert", lambda *a, **k: False)
    monkeypatch.setattr(api_enrichment, "fetch_salary_data", fake_fetch_salary_data)
    return {
        "pg": pg,
        "counts": counts,
        "delay": bls_delay,
        "bls": bls_result,
        "mp": monkeypatch,
        "tmp": tmp_path,
    }


_OTHER_SOURCES = [
    "_enrich_fred_economic",
    "_enrich_adzuna_jobs",
    "_enrich_census_demographics",
    "_enrich_job_posting_volume",
    "_enrich_benchmark_drift_check",
]


def _boot(world: dict[str, Any]) -> de.DataEnrichmentEngine:
    """A fresh process boot: new engine, state loaded from 'Supabase'.

    The non-BLS tasks are replaced by counting stubs that still go through the
    real ``_mark_refreshed`` (and therefore the real persistence path).
    """
    # A redeploy is a fresh container: Render's filesystem is ephemeral, so the
    # (gitignored) local state file does not survive it.
    world["tmp"].joinpath("no-such-state.json").unlink(missing_ok=True)
    eng = de.DataEnrichmentEngine()
    source_of = {
        "_enrich_fred_economic": "fred_economic",
        "_enrich_adzuna_jobs": "adzuna_jobs",
        "_enrich_census_demographics": "census_demographics",
        "_enrich_job_posting_volume": "job_posting_volume",
        "_enrich_benchmark_drift_check": "benchmark_drift_check",
    }
    for attr in _OTHER_SOURCES:
        src = source_of[attr]

        def make(src: str = src) -> Callable[[], None]:
            def task() -> None:
                if not eng._is_stale(src):
                    return
                world["counts"][src] = world["counts"].get(src, 0) + 1
                eng._mark_refreshed(src, True, 1)

            return task

        world["mp"].setattr(eng, attr, make())
    return eng


# ---------------------------------------------------------------------------
# 1. A redeploy does not re-fetch what was fetched before
# ---------------------------------------------------------------------------


def test_redeploy_does_not_refetch_bls(world: dict[str, Any]) -> None:
    boot1 = _boot(world)
    r1 = boot1.run_cycle()
    assert world["counts"]["bls"] == 1
    assert r1["refreshed"] == 6 and r1["skipped"] == 0

    boot2 = _boot(world)  # redeploy: fresh process, empty local state file
    r2 = boot2.run_cycle()
    assert world["counts"]["bls"] == 1, "BLS must not be hit again after a redeploy"
    assert r2["refreshed"] == 0 and r2["skipped"] == 6

    for _ in range(3):  # a burst of deploys
        _boot(world).run_cycle()
    assert world["counts"]["bls"] == 1
    for src in ("fred_economic", "adzuna_jobs", "census_demographics"):
        assert world["counts"][src] == 1


def test_the_refresh_is_actually_persisted_as_an_append_only_insert(
    world: dict[str, Any],
) -> None:
    _boot(world).run_cycle()
    sources = sorted(r["source"] for r in world["pg"].log_rows)
    # the six sources run_cycle() schedules (FRESHNESS_THRESHOLDS also lists a
    # legacy market_trends entry that has no task)
    assert sources == sorted(
        [
            "adzuna_jobs",
            "benchmark_drift_check",
            "bls_salary",
            "census_demographics",
            "fred_economic",
            "job_posting_volume",
        ]
    ), sources
    assert all(
        r["action"] == "refresh" and r["table_name"] == "data_enrichment"
        for r in world["pg"].log_rows
    )
    posts = [
        q
        for q in world["pg"].requests
        if q.get_method() == "POST" and q.full_url.endswith("/enrichment_log")
    ]
    assert posts, "enrichment_log writes go out as plain inserts"


def test_a_failed_refresh_is_persisted_too_so_a_redeploy_cannot_hammer_the_api(
    world: dict[str, Any],
) -> None:
    """BLS quota exhausted -> fetch returns nothing -> success=False is persisted;
    the next boot must not immediately retry (that was the 978-warning storm)."""
    world["bls"]["data"] = {}
    _boot(world).run_cycle()
    assert world["counts"]["bls"] == 1
    failed = [r for r in world["pg"].log_rows if r["source"] == "bls_salary"]
    assert len(failed) == 1 and json.loads(failed[0]["details"]) == {"success": False}
    _boot(world).run_cycle()
    _boot(world).run_cycle()
    assert world["counts"]["bls"] == 1


def test_state_loads_postgrest_timestamps_with_five_fraction_digits(
    world: dict[str, Any],
) -> None:
    _boot(world).run_cycle()
    eng = _boot(world)
    stamp = eng._state["last_runs"]["bls_salary"]
    assert stamp.endswith("+00:00") and len(stamp.split(".")[1].split("+")[0]) == 5
    assert (
        eng._is_stale("bls_salary") is False
    ), "unparseable timestamp must not mean stale"


@pytest.mark.parametrize(
    "stamp,expected_year",
    [
        ("2026-09-24T17:49:29.12345+00:00", 2026),
        ("2026-09-24T17:49:29.123456+00:00", 2026),
        ("2026-09-24T17:49:29+00:00", 2026),
        ("2026-09-24T17:49:29Z", 2026),
        ("2026-09-24T17:49:29.1Z", 2026),
        ("2026-09-24T17:49:29.123456789+00:00", 2026),
    ],
)
def test_parse_iso_handles_what_postgrest_and_python_emit(
    stamp: str, expected_year: int
) -> None:
    dt = de._parse_iso(stamp)
    assert dt is not None and dt.year == expected_year and dt.tzinfo is not None


def test_parse_iso_returns_none_for_garbage() -> None:
    assert de._parse_iso("") is None
    assert de._parse_iso("not a date") is None
    assert de._parse_iso(None) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 2. N+1 copies (master + forked workers) -> exactly one fetch
# ---------------------------------------------------------------------------


def test_five_engine_copies_cycling_at_once_fetch_bls_exactly_once(
    world: dict[str, Any],
) -> None:
    world["delay"]["s"] = 0.4  # the BLS pass is slow, so the copies overlap
    engines = [_boot(world) for _ in range(5)]  # state loaded BEFORE anyone refreshed
    barrier = threading.Barrier(5)
    results: list[dict[str, Any]] = []

    def cycle(e: de.DataEnrichmentEngine) -> None:
        barrier.wait()
        results.append(e.run_cycle())

    threads = [threading.Thread(target=cycle, args=(e,)) for e in engines]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert len(results) == 5
    assert world["counts"]["bls"] == 1, "one copy fetches; the others skip"
    for src in (
        "fred_economic",
        "adzuna_jobs",
        "census_demographics",
        "job_posting_volume",
    ):
        assert world["counts"][src] == 1, src
    bls_rows = [r for r in world["pg"].log_rows if r["source"] == "bls_salary"]
    assert len(bls_rows) == 1, "and only one persisted row"


def test_a_copy_that_lost_the_race_adopts_the_winners_state_next_cycle(
    world: dict[str, Any],
) -> None:
    follower = _boot(world)  # loaded empty state at boot
    winner = _boot(world)
    winner.run_cycle()
    assert world["counts"]["bls"] == 1
    assert follower._is_stale("bls_salary") is True, "follower's boot snapshot is stale"
    follower.run_cycle()  # re-reads persisted state at cycle start
    assert world["counts"]["bls"] == 1
    assert follower._is_stale("bls_salary") is False


def test_source_claim_blocks_a_second_claimant_until_released(
    world: dict[str, Any],
) -> None:
    release = de._claim_source("bls_salary")
    assert release is not None
    assert de._claim_source("bls_salary") is None, "held: another claimant is refused"
    assert de._claim_source("fred_economic") is not None, "claims are per source"
    release()
    again = de._claim_source("bls_salary")
    assert again is not None, "released: claimable again"
    again()


def test_claim_fails_open_when_the_lock_dir_is_unusable(
    world: dict[str, Any], tmp_path: Path
) -> None:
    blocker = tmp_path / "a-file"
    blocker.write_text("x", encoding="utf-8")
    world["mp"].setenv("NOVA_SLOT_DIR", str(blocker / "sub"))
    release = de._claim_source("bls_salary")
    assert release is not None, "no lock available -> proceed (never block enrichment)"
    release()


def test_the_claim_is_released_even_if_the_task_raises(world: dict[str, Any]) -> None:
    eng = _boot(world)

    def boom() -> None:
        raise RuntimeError("bls exploded")

    world["mp"].setattr(eng, "_enrich_bls_salary", boom)
    eng.run_cycle()
    again = de._claim_source("bls_salary")
    assert again is not None, "a crashed task must not leave the source locked"
    again()


# ---------------------------------------------------------------------------
# 3. State loader query
# ---------------------------------------------------------------------------


def test_state_loader_reads_a_generous_filtered_window(world: dict[str, Any]) -> None:
    _boot(world).run_cycle()
    gets = [q for q in world["pg"].requests if q.get_method() == "GET"]
    assert gets, "state was read from Supabase at boot"
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(gets[-1].full_url).query)
    assert qs["table_name"] == ["eq.data_enrichment"], "ignore other writers' rows"
    assert (
        int(qs["limit"][0]) >= 500
    ), "one chatty source must not evict the weekly ones"
    assert qs["order"] == ["created_at.desc"]


# ---------------------------------------------------------------------------
# 4. A FAILED refresh is retried on a short constant backoff, not after the
#    full success threshold (one transient BLS 429 must not skip BLS for 168 h)
# ---------------------------------------------------------------------------


def _age_persisted_rows(world: dict[str, Any], source: str, hours: float) -> None:
    """Make the persisted rows of ``source`` look ``hours`` old."""
    when = datetime.fromtimestamp(time.time() - hours * 3600, tz=timezone.utc)
    stamp = when.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-1] + "+00:00"
    for row in world["pg"].log_rows:
        if row["source"] == source:
            row["created_at"] = stamp


def test_failure_retry_window_is_a_short_constant() -> None:
    assert de._FAILURE_RETRY_HOURS == 6
    assert de._FAILURE_RETRY_HOURS < de.FRESHNESS_THRESHOLDS["bls_salary"]


def test_a_transient_bls_failure_is_retried_after_six_hours_not_a_week(
    world: dict[str, Any],
) -> None:
    world["bls"]["data"] = {}  # 429 / quota: the fetch returns nothing
    _boot(world).run_cycle()
    assert world["counts"]["bls"] == 1
    world["bls"]["data"] = {"Registered Nurse": {"median": 82000}}  # recovered

    _age_persisted_rows(world, "bls_salary", hours=7)
    _boot(world).run_cycle()  # next deploy, 7 h after the failure
    assert world["counts"]["bls"] == 2, "failure backoff elapsed: BLS is retried"

    _boot(world).run_cycle()  # success is persisted: another deploy skips it
    assert world["counts"]["bls"] == 2


def test_a_failure_is_not_retried_inside_the_backoff_window(
    world: dict[str, Any],
) -> None:
    world["bls"]["data"] = {}
    _boot(world).run_cycle()
    _age_persisted_rows(world, "bls_salary", hours=5)
    for _ in range(3):  # a deploy burst 5 h later
        _boot(world).run_cycle()
    assert world["counts"]["bls"] == 1, "still inside the 6 h window: no hammering"


def test_a_success_keeps_the_full_threshold_regardless_of_the_failure_window(
    world: dict[str, Any],
) -> None:
    _boot(world).run_cycle()  # BLS succeeds
    _age_persisted_rows(world, "bls_salary", hours=7)
    _boot(world).run_cycle()
    assert world["counts"]["bls"] == 1, "a success 7 h ago is fresh (168 h threshold)"
    _age_persisted_rows(
        world, "bls_salary", hours=de.FRESHNESS_THRESHOLDS["bls_salary"] + 1
    )
    _boot(world).run_cycle()
    assert world["counts"]["bls"] == 2, "past the success threshold: refreshed"


def test_is_stale_uses_the_failure_window_only_for_failed_attempts(
    world: dict[str, Any],
) -> None:
    eng = _boot(world)
    now = datetime.now(timezone.utc)

    def stamp(hours: float) -> str:
        return (now - timedelta(hours=hours)).isoformat()

    eng._state["last_runs"]["bls_salary"] = stamp(7)
    eng._state["last_success"] = {"bls_salary": False}
    assert eng._is_stale("bls_salary") is True
    eng._state["last_success"] = {"bls_salary": True}
    assert eng._is_stale("bls_salary") is False
    eng._state["last_runs"]["bls_salary"] = stamp(5)
    eng._state["last_success"] = {"bls_salary": False}
    assert eng._is_stale("bls_salary") is False
    # legacy state without the flag (old local file) is treated as a success
    eng._state.pop("last_success")
    eng._state["last_runs"]["bls_salary"] = stamp(7)
    assert eng._is_stale("bls_salary") is False


def test_adopted_state_carries_the_success_flag_from_the_persisted_row(
    world: dict[str, Any],
) -> None:
    world["bls"]["data"] = {}
    _boot(world).run_cycle()  # persisted: bls_salary failed
    fresh_boot = _boot(world)
    assert fresh_boot._state["last_success"]["bls_salary"] is False
    assert fresh_boot._state["last_success"]["fred_economic"] is True
    status = fresh_boot.get_status()["freshness"]["bls_salary"]
    assert status["last_run_ok"] is False
    assert status["retry_hours_after_failure"] == de._FAILURE_RETRY_HOURS
