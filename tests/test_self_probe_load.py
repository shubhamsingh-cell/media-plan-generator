"""Regression tests: the app's self-probe load (AutoQC) and /api/dashboard/widgets.

Prod telemetry 2026-10-01 (ledger A_prod_telemetry section 2 row 3 / section 6):

* AutoQC is documented as one 5-endpoint probe cycle every 60 s, yet prod logged
  ~294 loopback requests per hour per endpoint (~4.9/min = ~24 requests/min) --
  99.2% of all log lines. Under ``gunicorn --preload`` + gevent the monitor
  greenlet started at import is inherited by EVERY forked worker
  (wsgi.py documents this: "re-runs to completion independently in the master
  AND in EVERY forked worker"), so master + 4 workers = 5 live loops. The
  flock leader election in app.py runs once, in the master, before the fork, so
  it cannot stop the inherited copies.
* ``GET /api/dashboard/widgets`` took 1-3.6 s on every probe (605 "SLOW
  ENDPOINT" warnings/week) because each request made a live Supabase
  ``get_market_trends()`` read.

Fixes pinned here:

* AutoQC probes are shared instance-wide through a tiny per-endpoint file in the
  slot dir: however many loops are alive, each endpoint is really probed at most
  once per ``_CHECK_INTERVAL`` (60 s). Every loop still computes its own
  results, so /api/health/auto-qc stays truthful in whichever worker serves it.
* The widgets payload is cached (60 s fresh, served stale up to 10 min while one
  background refresh runs) so probes and users never wait on Supabase in steady
  state. The liveness ping is untouched and stays uncached.
"""

from __future__ import annotations

import io
import json
import threading
from pathlib import Path
from typing import Any

import pytest

import auto_qc
from routes import health as health_routes


class _Clock:
    """Stand-in for the ``time`` module: time/monotonic advance only on demand."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now

    def sleep(self, _s: float) -> None:
        return None


# ===========================================================================
# 1. AutoQC: instance-wide probe sharing
# ===========================================================================


class _Http200:
    status = 200

    def __enter__(self) -> "_Http200":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def read(self) -> bytes:
        return b"{}"


class _CountingUrlopen:
    def __init__(self, fail: bool = False) -> None:
        self.urls: list[str] = []
        self.fail = fail

    def __call__(self, url: str, timeout: Any = None) -> Any:
        self.urls.append(url)
        if self.fail:
            raise OSError("connection refused")
        return _Http200()


@pytest.fixture()
def probe_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, Any]:
    clock = _Clock()
    net = _CountingUrlopen()
    monkeypatch.setenv("NOVA_SLOT_DIR", str(tmp_path))
    monkeypatch.setenv("PORT", "10000")
    monkeypatch.setattr(auto_qc, "time", clock)
    monkeypatch.setattr(auto_qc.urllib.request, "urlopen", net)
    return {"clock": clock, "net": net, "dir": tmp_path, "mp": monkeypatch}


def test_documented_cadence_is_sixty_seconds() -> None:
    assert auto_qc._CHECK_INTERVAL >= 60
    paths = [p for _n, p in auto_qc._check_definitions]
    assert "/api/health/ping" in paths and "/api/dashboard/widgets" in paths


def test_five_loops_probing_together_make_one_real_request(
    probe_env: dict[str, Any],
) -> None:
    """The bug: master + 4 forked workers each probed every cycle (5x load)."""
    net = probe_env["net"]
    results = [auto_qc._probe("/api/channels") for _ in range(5)]  # 5 "processes"
    assert len(net.urls) == 1, net.urls
    assert all(ok for ok, _lat in results)
    assert len({r for r in results}) == 1, "followers reuse the leader's reading"


def test_a_full_five_endpoint_cycle_from_five_loops_is_five_requests_not_twenty_five(
    probe_env: dict[str, Any],
) -> None:
    net = probe_env["net"]
    for _loop in range(5):
        for _name, path in auto_qc._check_definitions:
            auto_qc._probe(path)
    assert len(net.urls) == len(auto_qc._check_definitions)


def test_probe_repeats_only_after_the_interval(probe_env: dict[str, Any]) -> None:
    clock, net = probe_env["clock"], probe_env["net"]
    auto_qc._probe("/")
    clock.now += auto_qc._CHECK_INTERVAL - 1
    auto_qc._probe("/")
    assert len(net.urls) == 1, "inside the interval: reuse"
    clock.now += 2  # now 61 s after the real probe
    auto_qc._probe("/")
    assert len(net.urls) == 2, "interval elapsed: one real probe again"


def test_failures_are_shared_too(probe_env: dict[str, Any]) -> None:
    probe_env["mp"].setattr(
        auto_qc.urllib.request, "urlopen", _CountingUrlopen(fail=True)
    )
    net = auto_qc.urllib.request.urlopen
    ok1, _ = auto_qc._probe("/api/health/ready")
    ok2, _ = auto_qc._probe("/api/health/ready")
    assert ok1 is False and ok2 is False
    # one probe = 2 attempts (the existing silent retry); the second loop adds none
    assert len(net.urls) == 2


def test_each_endpoint_is_cached_independently(probe_env: dict[str, Any]) -> None:
    net = probe_env["net"]
    auto_qc._probe("/")
    auto_qc._probe("/api/channels")
    auto_qc._probe("/")
    assert [u.rsplit("10000", 1)[1] for u in net.urls] == ["/", "/api/channels"]


def test_entry_older_than_the_interval_on_disk_is_not_reused(
    probe_env: dict[str, Any],
) -> None:
    clock, net = probe_env["clock"], probe_env["net"]
    auto_qc._probe("/")
    clock.now += 3600
    auto_qc._probe("/")
    assert len(net.urls) == 2


def test_corrupt_shared_file_falls_back_to_a_direct_probe(
    probe_env: dict[str, Any],
) -> None:
    net = probe_env["net"]
    auto_qc._probe("/")
    for f in probe_env["dir"].glob("auto_qc_probe_*"):
        f.write_text("{not json", encoding="utf-8")
    ok, _lat = auto_qc._probe("/")
    assert ok is True
    assert len(net.urls) == 2, "corrupt cache ignored, real probe made, file repaired"
    assert auto_qc._probe("/")[0] is True
    assert len(net.urls) == 2


def test_unusable_slot_dir_never_breaks_probing(
    probe_env: dict[str, Any], tmp_path: Path
) -> None:
    blocker = tmp_path / "file-not-dir"
    blocker.write_text("x", encoding="utf-8")
    probe_env["mp"].setenv("NOVA_SLOT_DIR", str(blocker / "sub"))
    net = probe_env["net"]
    assert auto_qc._probe("/")[0] is True
    assert auto_qc._probe("/")[0] is True
    assert len(net.urls) == 2, "no shared cache available: probes directly, no error"


def test_check_cycle_from_many_loops_keeps_per_process_results(
    probe_env: dict[str, Any],
) -> None:
    """Every loop still builds its own results (status stays per-process truthful)."""
    mp = probe_env["mp"]
    mp.setattr(auto_qc, "_check_history", auto_qc.deque(maxlen=1440))
    mp.setattr(auto_qc, "_check_count", 0)
    cycles = [auto_qc._check_cycle() for _ in range(5)]
    assert len(auto_qc._check_history) == 5
    assert all(
        set(c["checks"]) == {n for n, _ in auto_qc._check_definitions} for c in cycles
    )
    assert all(c["health_score"] == 1.0 for c in cycles)
    assert len(probe_env["net"].urls) == len(auto_qc._check_definitions)


# ---------------------------------------------------------------------------
# 1b. The per-endpoint try-lock: simultaneous loops probe exactly once
# ---------------------------------------------------------------------------
# A shared result file alone only de-duplicates loops that happen to be staggered:
# processes that start together all read "stale" at the same instant and all
# probe (independent verification: 4 simultaneous processes made 4 real probes,
# a staggered start made 1). The flock makes exactly one of them probe.


class _CountingServer:
    """Real HTTP server that counts hits per path (and takes a moment to answer
    so concurrent probes genuinely overlap)."""

    def __init__(self, delay_s: float = 0.25) -> None:
        import http.server
        import socketserver

        self.hits: dict[str, int] = {}
        self._lock = threading.Lock()
        outer = self

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                with outer._lock:
                    outer.hits[self.path] = outer.hits.get(self.path, 0) + 1
                import time

                time.sleep(delay_s)
                body = b"{}"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a: Any) -> None:
                return None

        class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        self.server = _Server(("127.0.0.1", 0), _Handler)
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


_PROBER = r"""
import json, os, sys, time
sys.path.insert(0, os.environ["ROOT"])
import auto_qc

auto_qc._PROBE_SHARE_TTL_S = float(os.environ["TTL"])
start_at = float(os.environ["START_AT"])
gap = float(os.environ["GAP"])
out = []
for rnd in range(int(os.environ["ROUNDS"])):
    while time.time() < start_at + rnd * gap:
        time.sleep(0.002)
    for _name, path in auto_qc._check_definitions:
        ok, latency = auto_qc._probe(path, timeout=5)
        out.append([rnd, path, ok])
print(json.dumps(out))
"""


def _run_prober_processes(
    port: int, slot_dir: Path, procs: int, rounds: int, ttl: float, gap: float
) -> list[list]:
    import os
    import subprocess
    import sys
    import time

    env = dict(os.environ)
    env.update(
        {
            "ROOT": str(Path(__file__).resolve().parent.parent),
            "PORT": str(port),
            "NOVA_SLOT_DIR": str(slot_dir),
            "TTL": str(ttl),
            "GAP": str(gap),
            "ROUNDS": str(rounds),
            "START_AT": str(time.time() + 1.5),
        }
    )
    children = [
        subprocess.Popen(
            [sys.executable, "-c", _PROBER],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(procs)
    ]
    results: list[list] = []
    for child in children:
        out, err = child.communicate(timeout=120)
        assert child.returncode == 0, err
        results.extend(json.loads(out))
    return results


def test_four_real_processes_started_together_make_one_probe_per_endpoint_per_window(
    tmp_path: Path,
) -> None:
    server = _CountingServer(delay_s=0.25)
    try:
        results = _run_prober_processes(
            server.port, tmp_path, procs=4, rounds=2, ttl=1.0, gap=2.5
        )
    finally:
        server.close()
    paths = [p for _n, p in auto_qc._check_definitions]
    # 2 windows (the second starts after the first reading expired): 1 probe each
    assert server.hits == {p: 2 for p in paths}, server.hits
    assert len(results) == 4 * 2 * len(paths)
    assert all(ok for _r, _p, ok in results), "followers got the leader's reading"


def test_single_window_four_processes_is_exactly_one_request_per_endpoint(
    tmp_path: Path,
) -> None:
    server = _CountingServer(delay_s=0.25)
    try:
        _run_prober_processes(server.port, tmp_path, procs=4, rounds=1, ttl=30.0, gap=0)
    finally:
        server.close()
    paths = [p for _n, p in auto_qc._check_definitions]
    assert server.hits == {p: 1 for p in paths}, server.hits


@pytest.fixture()
def real_clock_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, Any]:
    net = _CountingUrlopen()
    monkeypatch.setenv("NOVA_SLOT_DIR", str(tmp_path))
    monkeypatch.setenv("PORT", "10000")
    monkeypatch.setattr(auto_qc.urllib.request, "urlopen", net)
    return {"net": net, "dir": tmp_path}


def test_follower_waits_for_the_lock_holders_reading_instead_of_probing(
    real_clock_env: dict[str, Any],
) -> None:
    release = auto_qc._try_probe_lock("/api/channels")
    assert release is not None
    assert auto_qc._try_probe_lock("/api/channels") is None, "held: refused"
    answer: list[tuple[bool, float]] = []
    follower = threading.Thread(
        target=lambda: answer.append(auto_qc._probe("/api/channels"))
    )
    follower.start()
    threading.Event().wait(0.3)  # follower is polling, not probing
    assert real_clock_env["net"].urls == []
    auto_qc._write_shared_probe("/api/channels", True, 42.0)  # the leader publishes
    release()
    follower.join(timeout=5)
    assert answer == [(True, 42.0)]
    assert real_clock_env["net"].urls == [], "the follower never made an HTTP request"


def test_follower_takes_over_when_the_leader_dies_without_publishing(
    real_clock_env: dict[str, Any],
) -> None:
    release = auto_qc._try_probe_lock("/api/channels")
    assert release is not None
    answer: list[tuple[bool, float]] = []
    follower = threading.Thread(
        target=lambda: answer.append(auto_qc._probe("/api/channels"))
    )
    follower.start()
    threading.Event().wait(0.3)
    release()  # leader gone: lock freed, nothing published
    follower.join(timeout=10)
    assert answer and answer[0][0] is True
    assert len(real_clock_env["net"].urls) == 1, "exactly one takeover probe"


def test_probe_lock_is_per_endpoint(real_clock_env: dict[str, Any]) -> None:
    a = auto_qc._try_probe_lock("/api/channels")
    b = auto_qc._try_probe_lock("/")
    assert a is not None and b is not None
    a()
    b()


def test_lock_unavailable_fails_open_and_still_probes(
    real_clock_env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(auto_qc, "fcntl", None)
    assert auto_qc._try_probe_lock("/") is not None
    assert auto_qc._probe("/")[0] is True
    assert len(real_clock_env["net"].urls) == 1


# ===========================================================================
# 2. /api/dashboard/widgets: cached, stale-while-revalidate, single-flight
# ===========================================================================


class _Wfile(io.BytesIO):
    pass


class _FakeHandler:
    def __init__(self) -> None:
        self.wfile = _Wfile()
        self.status = 0
        self.headers_out: dict[str, str] = {}

    def send_response(self, code: int) -> None:
        self.status = code

    def send_header(self, k: str, v: str) -> None:
        self.headers_out[k] = v

    def end_headers(self) -> None:
        return None

    def _get_cors_origin(self) -> str:
        return ""

    def body(self) -> dict[str, Any]:
        return json.loads(self.wfile.getvalue())


@pytest.fixture()
def widgets_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    import app

    calls = {"n": 0}
    gate = threading.Event()
    gate.set()
    started = threading.Event()

    def fake_trends(*_a: Any, **_k: Any) -> list[dict[str, Any]]:
        calls["n"] += 1
        started.set()
        gate.wait(timeout=10)
        return [{"t": i} for i in range(5)]

    clock = _Clock()
    monkeypatch.setattr(app, "_supabase_data_available", True, raising=False)
    monkeypatch.setattr(app, "get_market_trends", fake_trends, raising=False)
    monkeypatch.setattr(health_routes, "time", clock)
    monkeypatch.setattr(
        health_routes,
        "_widgets_entry",
        {"ts": 0.0, "value": None, "refreshing": False},
        raising=False,
    )
    return {"calls": calls, "gate": gate, "started": started, "clock": clock}


def _get_widgets() -> _FakeHandler:
    h = _FakeHandler()
    health_routes._handle_dashboard_widgets(h, "/api/dashboard/widgets", None)
    return h


def test_widgets_payload_shape_is_unchanged(widgets_env: dict[str, Any]) -> None:
    h = _get_widgets()
    assert h.status == 200
    body = h.body()
    assert set(body) == {
        "campaigns",
        "budget",
        "market",
        "compliance",
        "recent_activity",
    }
    assert body["market"]["trend"] == "growing"
    assert body["market"]["label"] == "5 active market signals"


def test_repeated_probes_inside_the_ttl_do_not_hit_supabase(
    widgets_env: dict[str, Any],
) -> None:
    for _ in range(12):
        assert _get_widgets().status == 200
        widgets_env["clock"].now += 4
    assert widgets_env["calls"]["n"] == 1, "one live read serves the whole window"


def test_stale_value_is_served_instantly_while_one_background_refresh_runs(
    widgets_env: dict[str, Any],
) -> None:
    _get_widgets()  # cold build
    assert widgets_env["calls"]["n"] == 1
    widgets_env["clock"].now += health_routes._WIDGETS_FRESH_TTL_S + 1
    widgets_env["gate"].clear()  # make the refresh slow (simulates 3 s Supabase)
    widgets_env["started"].clear()

    first = _get_widgets()  # must NOT block on the slow refresh
    assert first.status == 200
    assert widgets_env["started"].wait(timeout=5), "background refresh started"
    second = _get_widgets()
    third = _get_widgets()
    assert second.status == 200 and third.status == 200
    widgets_env["gate"].set()
    deadline = 50
    while widgets_env["calls"]["n"] < 2 and deadline:
        threading.Event().wait(0.05)
        deadline -= 1
    threading.Event().wait(0.1)
    assert widgets_env["calls"]["n"] == 2, "exactly one refresh for three stale hits"


def test_value_older_than_the_stale_window_is_rebuilt_synchronously(
    widgets_env: dict[str, Any],
) -> None:
    _get_widgets()
    widgets_env["clock"].now += health_routes._WIDGETS_STALE_MAX_S + 1
    _get_widgets()
    assert widgets_env["calls"]["n"] == 2


def test_concurrent_cold_requests_share_one_build(widgets_env: dict[str, Any]) -> None:
    widgets_env["gate"].clear()
    statuses: list[int] = []

    def worker() -> None:
        statuses.append(_get_widgets().status)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    assert widgets_env["started"].wait(timeout=5)
    widgets_env["gate"].set()
    for t in threads:
        t.join(timeout=10)
    assert statuses == [200] * 8
    assert (
        widgets_env["calls"]["n"] == 1
    ), "single-flight: 8 cold probes, 1 Supabase read"


def test_failed_build_is_reported_and_not_cached(
    widgets_env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom() -> dict[str, Any]:
        raise RuntimeError("boom")

    monkeypatch.setattr(health_routes, "_build_dashboard_widgets", boom)
    h = _get_widgets()
    assert h.status == 500 and "boom" in h.body()["error"]
    assert health_routes._widgets_entry["value"] is None


def test_liveness_ping_is_not_cached() -> None:
    h1, h2 = _FakeHandler(), _FakeHandler()
    health_routes._handle_health_ping(h1, "/api/health/ping", None)
    health_routes._handle_health_ping(h2, "/api/health/ping", None)
    assert h1.status == h2.status == 200
    assert h1.body()["status"] == "ok"
    assert not hasattr(health_routes, "_ping_cache")


def test_market_fetch_failure_still_returns_default_widgets(
    widgets_env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import app

    def bad(*_a: Any, **_k: Any) -> list:
        raise OSError("supabase down")

    monkeypatch.setattr(app, "get_market_trends", bad, raising=False)
    h = _get_widgets()
    assert h.status == 200
    assert h.body()["market"]["label"] == "Labor market trends steady"
