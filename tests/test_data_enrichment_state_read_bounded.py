"""The enrichment_log state read must stay bounded and cheap.

Production logged "All 3 retries exhausted for _fetch_enrichment_log: The read
operation timed out" ~40 times per 48 h: the per-cycle re-read of the
append-only ``enrichment_log`` table was an unbounded scan with a 15 s timeout
and 3 retries (2+4+8 s backoff), blocking the enrichment thread ~74 s per cycle.

Pinned here:
- the request carries a ``created_at=gte.<ISO>`` bound computed at call time;
- a timeout leaves ``self._state`` untouched;
- the per-cycle (best-effort) read uses a single retry and a shorter timeout,
  and logs exhaustion as a WARNING ("best-effort state read"), not an ERROR;
- the boot-time read keeps the full retry/timeout budget.
"""

from __future__ import annotations

import logging
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import data_enrichment as de

BASE = "https://example.supabase.co"


class _Resp:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def read(self, *a: Any) -> bytes:
        return self._body


class _Net:
    """urlopen stand-in recording url + timeout; raises or returns rows."""

    def __init__(self, body: bytes = b"[]", exc: Exception | None = None) -> None:
        self.body = body
        self.exc = exc
        self.calls: list[tuple[str, Any]] = []

    def __call__(self, req: Any, timeout: Any = None, context: Any = None) -> Any:
        self.calls.append((req.full_url, timeout))
        if self.exc is not None:
            raise self.exc
        return _Resp(self.body)


@pytest.fixture()
def engine(monkeypatch: pytest.MonkeyPatch) -> de.DataEnrichmentEngine:
    monkeypatch.setattr(de, "_SUPABASE_ENABLED", False)
    eng = de.DataEnrichmentEngine()
    monkeypatch.setattr(de, "_SUPABASE_ENABLED", True)
    monkeypatch.setattr(de, "SUPABASE_URL", BASE)
    monkeypatch.setattr(de, "SUPABASE_KEY", "test-key")
    sleeps: list[float] = []
    monkeypatch.setattr(de.time, "sleep", lambda s: sleeps.append(s))
    eng._sleeps = sleeps  # type: ignore[attr-defined]
    return eng


def _since(url: str) -> datetime:
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
    (val,) = query["created_at"]
    assert val.startswith("gte."), val
    return datetime.strptime(val[4:], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )


def test_request_url_has_created_at_lower_bound(
    engine: de.DataEnrichmentEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    net = _Net()
    monkeypatch.setattr(de.urllib.request, "urlopen", net)
    engine._load_state_from_supabase(best_effort=True)
    url = net.calls[0][0]
    assert "created_at=gte." in url
    assert "table_name=eq.data_enrichment" in url
    # Per-cycle window is ~14 days back, computed now (not an import-time constant).
    age = datetime.now(timezone.utc) - _since(url)
    assert timedelta(days=13, hours=23) < age < timedelta(days=14, minutes=5)
    # The "+"/":" characters must be percent-encoded or PostgREST misparses them.
    raw = url.split("created_at=gte.")[1].split("&")[0]
    assert ":" not in raw and "+" not in raw


def test_boot_read_is_bounded_but_covers_slowest_source(
    engine: de.DataEnrichmentEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    net = _Net()
    monkeypatch.setattr(de.urllib.request, "urlopen", net)
    engine._load_state_from_supabase()
    age = datetime.now(timezone.utc) - _since(net.calls[0][0])
    longest = timedelta(hours=max(de.FRESHNESS_THRESHOLDS.values()))
    assert age > longest  # a redeploy must not make quarterly sources look never-run
    # ...but it must stay bounded: no more than the longest threshold + 1 day.
    assert age <= longest + timedelta(days=1, minutes=5)


def test_timeout_leaves_state_untouched(
    engine: de.DataEnrichmentEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine._state = {
        "last_runs": {"bls_salary": "2026-10-30T00:00:00+00:00"},
        "last_success": {"bls_salary": True},
        "stats": {"total_enrichments": 1, "total_failures": 0},
    }
    before = {
        "last_runs": dict(engine._state["last_runs"]),
        "last_success": dict(engine._state["last_success"]),
    }
    monkeypatch.setattr(
        de.urllib.request, "urlopen", _Net(exc=TimeoutError("The read operation timed out"))
    )
    engine._adopt_persisted_state()
    assert engine._state["last_runs"] == before["last_runs"]
    assert engine._state["last_success"] == before["last_success"]


def test_per_cycle_read_uses_one_retry_and_short_timeout(
    engine: de.DataEnrichmentEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    net = _Net(exc=TimeoutError("The read operation timed out"))
    monkeypatch.setattr(de.urllib.request, "urlopen", net)
    engine._adopt_persisted_state()
    assert len(net.calls) == 2  # first attempt + exactly one retry
    assert all(t == 8 for _, t in net.calls)
    assert len(engine._sleeps) == 1  # type: ignore[attr-defined]


def test_boot_read_keeps_full_retry_budget(
    engine: de.DataEnrichmentEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    net = _Net(exc=TimeoutError("The read operation timed out"))
    monkeypatch.setattr(de.urllib.request, "urlopen", net)
    assert engine._load_state_from_supabase() is None
    assert len(net.calls) == 4  # 1 + 3 retries
    assert all(t == de._HTTP_TIMEOUT for _, t in net.calls)


def test_exhausted_per_cycle_read_logs_warning_not_error(
    engine: de.DataEnrichmentEngine,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        de.urllib.request, "urlopen", _Net(exc=TimeoutError("The read operation timed out"))
    )
    with caplog.at_level(logging.DEBUG, logger=de.logger.name):
        engine._adopt_persisted_state()
    exhausted = [r for r in caplog.records if "retries exhausted" in r.getMessage()]
    assert len(exhausted) == 1
    assert exhausted[0].levelno == logging.WARNING
    assert "best-effort state read" in exhausted[0].getMessage()
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def _pg_rows(days_ago: dict[str, int]) -> bytes:
    import json

    now = datetime.now(timezone.utc)
    return json.dumps(
        [
            {
                "source": src,
                "records_affected": 1,
                "details": {"success": True},
                "created_at": (now - timedelta(days=d)).isoformat(),
            }
            for src, d in days_ago.items()
        ]
    ).encode()


class _WindowedNet(_Net):
    """Serves only the rows inside the request's created_at lower bound."""

    def __init__(self, rows: list[dict]) -> None:
        super().__init__()
        self.rows = rows

    def __call__(self, req: Any, timeout: Any = None, context: Any = None) -> Any:
        import json

        self.calls.append((req.full_url, timeout))
        since = _since(req.full_url)
        kept = [
            r
            for r in self.rows
            if datetime.fromisoformat(r["created_at"]) >= since
        ]
        return _Resp(json.dumps(kept).encode())


def test_failed_boot_read_still_recovers_monthly_and_quarterly_sources(
    engine: de.DataEnrichmentEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Boot read timed out -> state empty. The per-cycle read must widen to the
    boot window so 20 d / 40 d old runs are found (census 720 h, drift 2160 h);
    otherwise a redeploy re-runs them and burns Census/Adzuna quota."""
    import json

    engine._state = {"last_runs": {}, "stats": {}}
    rows = json.loads(
        _pg_rows({"census_demographics": 20, "benchmark_drift_check": 40})
    )
    net = _WindowedNet(rows)
    monkeypatch.setattr(de.urllib.request, "urlopen", net)
    engine._adopt_persisted_state()
    assert not engine._is_stale("census_demographics")
    assert not engine._is_stale("benchmark_drift_check")
    # still best-effort: single attempt succeeded, short timeout
    assert net.calls[0][1] == de._STATE_BEST_EFFORT_TIMEOUT


def test_complete_state_uses_the_short_window(
    engine: de.DataEnrichmentEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    engine._state = {
        "last_runs": {src: now for src in de.FRESHNESS_THRESHOLDS},
        "stats": {},
    }
    net = _Net()
    monkeypatch.setattr(de.urllib.request, "urlopen", net)
    engine._adopt_persisted_state()
    age = datetime.now(timezone.utc) - _since(net.calls[0][0])
    assert age < timedelta(days=15)
