"""Regression tests: log_redaction must be linear-time on adversarial input.

Independent verification of mpg-ops-hygiene@427ef70 found a ReDoS in the
``_USERINFO`` pattern: it began with a scheme matcher
(``[A-Za-z][A-Za-z0-9+.\\-]*://``) tried at EVERY position of a long run of scheme
characters, so a run of N characters cost O(N^2): 10 KB 0.12 s, 40 KB 2 s, 80 KB
7 s. The log filter runs on every formatted message and exception text, and
request bodies up to 10 MB are accepted, so an unauthenticated
``POST /api/optimize {"budget": "key " + "a" * 40000}`` (any message containing
the trigger word ``key``/``token``/... and a long run of letters reaches the
pattern through the error log) froze a gevent worker for the whole quadratic
scan -- 2.75 s at 40 KB on this machine, 29.6 s on the verifier's.

Fixes pinned here:

* every pattern is linear: URL userinfo is anchored on the literal ``://`` and
  no longer scans a scheme prefix, so it also cannot miss odd schemes;
* ``redact_secrets`` scans at most ``_MAX_SCAN_CHARS`` (64 KB) and replaces the
  unscanned tail with a truncation marker -- a secret in the tail cannot leak
  and an unbounded line cannot burn CPU.

The timing tests run in a subprocess with a hard timeout so that a regression
FAILS fast instead of hanging the suite (``re`` cannot be interrupted).
"""

from __future__ import annotations

import http.client
import json
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from log_redaction import REDACTED, SecretRedactingFilter, redact_secrets

PROJECT_ROOT = Path(__file__).resolve().parent.parent

REGEX_BUDGET_S = 0.25  # one pattern, 1 MB adversarial line
FILTER_BUDGET_S = 0.5  # whole redact_secrets / logging filter, 1 MB line
SUBPROCESS_TIMEOUT_S = 60

_HARNESS_BODY = r"""
import json, logging, sys, time
sys.path.insert(0, ROOT)
import log_redaction as lr

N = 1_000_000
inputs = {
    "key + a*N": "key " + "a" * N,
    "a*N": "a" * N,
    "key=*N": "key=" * (N // 4),
    "=*N": "=" * N,
    "&*N": "&" * N,
    "a:*N": "a:" * (N // 2),
    "http://*N": "http://" * (N // 7),
    "http://a:*N": "http://a:" * (N // 9),
    "base64": "QUJDREVGRw==" * (N // 12),
    "'*N": "'" * N,
    "dq key sq": "\"key\":'" * (N // 7),
    "sq key sq": "'key': '" * (N // 8),
    "authorization: *N": "authorization: " * (N // 15),
    "bearer *N": "bearer " * (N // 7),
    "bearer a*N": "bearer " + "a" * N,
    "bearer spaces": "bearer" + " " * N,
    "token*N": "token" * (N // 5),
    "key*N": "key" * (N // 3),
    "k=a&*N": "k=a&" * (N // 4),
    "a-b.c+d*N": "a-b.c+d" * (N // 7),
    "://u@*N": "://u@" * (N // 5),
    "://:*N": "://" + ":" * N,
    "://a:b*N": "://a:" + "b" * N,
    "a*N then userinfo": "key " + "a" * N + "://u:pw@host",
    "spaces then key=": " " * N + "key=abc",
    "mixed": ("key=abc&token=zz Authorization: Bearer abcdefgh.ijk "
              "postgres://u:p@h/db {'api_key': 'x'} ") * (N // 90),
}
patterns = {
    "TRIGGER": lr._TRIGGER, "PARAM": lr._PARAM, "QUOTED": lr._QUOTED,
    "HEADER": lr._HEADER, "BEARER": lr._BEARER, "USERINFO": lr._USERINFO,
}
out = []
for pname, pat in patterns.items():
    for iname, text in inputs.items():
        best = 1e9
        for _attempt in range(2):  # min of 2: first touch of a 1 MB string is noisy
            t0 = time.perf_counter()
            for _ in pat.finditer(text):
                pass
            best = min(best, time.perf_counter() - t0)
        out.append(["regex", pname, iname, best])
        print(json.dumps(out[-1]), flush=True)
flt = lr.SecretRedactingFilter()
for iname, text in inputs.items():
    best = 1e9
    for _attempt in range(2):
        t0 = time.perf_counter()
        lr.redact_secrets(text)
        best = min(best, time.perf_counter() - t0)
    out.append(["redact_secrets", "-", iname, best])
    print(json.dumps(out[-1]), flush=True)
    best = 1e9
    for _attempt in range(2):
        rec = logging.LogRecord("n", logging.WARNING, "x.py", 1, "%s", (text,), None)
        t0 = time.perf_counter()
        flt.filter(rec)
        best = min(best, time.perf_counter() - t0)
    out.append(["filter", "-", iname, best])
    print(json.dumps(out[-1]), flush=True)
"""
_HARNESS = f"ROOT = {str(PROJECT_ROOT)!r}\n" + _HARNESS_BODY


@pytest.fixture(scope="module")
def timings() -> list[list]:
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _HARNESS],
            capture_output=True,
            text=True,
            timeout=SUBPROCESS_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        done = (exc.stdout or b"").decode("utf-8", "replace").strip().splitlines()
        last = done[-1] if done else "(no case finished)"
        pytest.fail(
            f"adversarial log-redaction harness exceeded {SUBPROCESS_TIMEOUT_S}s "
            f"(quadratic/catastrophic regex). Last finished case: {last}"
        )
    assert proc.returncode == 0, proc.stderr
    return [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]


def test_every_regex_is_fast_on_every_adversarial_megabyte_line(
    timings: list[list],
) -> None:
    slow = [t for t in timings if t[0] == "regex" and t[3] > REGEX_BUDGET_S]
    assert not slow, [f"{p}:{i} {s * 1000:.0f}ms" for _k, p, i, s in slow]
    assert len([t for t in timings if t[0] == "regex"]) >= 6 * 20


def test_redact_secrets_is_fast_on_every_adversarial_megabyte_line(
    timings: list[list],
) -> None:
    slow = [t for t in timings if t[0] == "redact_secrets" and t[3] > FILTER_BUDGET_S]
    assert not slow, [f"{i} {s * 1000:.0f}ms" for _k, _p, i, s in slow]


def test_logging_filter_is_fast_on_every_adversarial_megabyte_line(
    timings: list[list],
) -> None:
    slow = [t for t in timings if t[0] == "filter" and t[3] > FILTER_BUDGET_S]
    assert not slow, [f"{i} {s * 1000:.0f}ms" for _k, _p, i, s in slow]


# ---------------------------------------------------------------------------
# Bounded scanning: redact-by-truncation beyond the cap
# ---------------------------------------------------------------------------


def test_cap_constant_is_64_kb() -> None:
    import log_redaction

    assert log_redaction._MAX_SCAN_CHARS == 65536


def test_text_within_the_cap_is_untouched_by_the_cap() -> None:
    import log_redaction

    text = "x" * (log_redaction._MAX_SCAN_CHARS - 40) + " key=SECRETVALUE"
    out = redact_secrets(text)
    assert "SECRETVALUE" not in out
    assert "truncated" not in out
    assert len(text.split()) == len(out.split())


def test_oversized_text_is_truncated_not_scanned_and_tail_secrets_cannot_leak() -> None:
    import log_redaction

    cap = log_redaction._MAX_SCAN_CHARS
    head_secret = "api_key=HEADHEADHEAD "
    tail_secret = " api_key=TAILTAILTAIL"
    text = head_secret + "a" * (cap * 3) + tail_secret
    out = redact_secrets(text)
    assert "HEADHEADHEAD" not in out and f"api_key={REDACTED}" in out
    assert "TAILTAILTAIL" not in out, "unscanned tail must be dropped, not emitted raw"
    assert len(out) < cap + 200
    assert "truncated" in out and str(len(text) - cap) in out


def test_secret_split_by_the_cap_boundary_is_still_redacted() -> None:
    import log_redaction

    cap = log_redaction._MAX_SCAN_CHARS
    text = "a" * (cap - 12) + " api_key=ABCDEFGHIJKLMNOPQRSTUV and more"
    out = redact_secrets(text)
    assert "ABCDEFGH" not in out, out[-80:]


def test_filter_applies_the_same_cap_to_exception_text() -> None:
    import log_redaction

    cap = log_redaction._MAX_SCAN_CHARS
    try:
        raise RuntimeError("boom " + "a" * (cap * 2) + " api_key=TAILTAILTAIL")
    except RuntimeError:
        import logging

        rec = logging.LogRecord(
            "n", logging.ERROR, __file__, 1, "failed", None, sys.exc_info()
        )
    SecretRedactingFilter().filter(rec)
    assert rec.exc_text and "TAILTAILTAIL" not in rec.exc_text
    assert len(rec.exc_text) < cap + 1000


# ---------------------------------------------------------------------------
# URL userinfo: still redacted for every scheme shape (anchored on '://')
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "postgresql://postgres:pw123abc@db.host:5432/app",
        "(see postgres://u:pw123abc@h/db)",
        "xhttp://u:pw123abc@h",
        "1-http://u:pw123abc@h",
        "redis+ssl://default:pw123abc@cache:6379",
        "a.b-c+d://u:pw123abc@h",
    ],
)
def test_userinfo_password_redacted_for_any_scheme_shape(text: str) -> None:
    out = redact_secrets(text)
    assert "pw123abc" not in out
    assert f":{REDACTED}@" in out


# ---------------------------------------------------------------------------
# End to end through the REAL request handler
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def live_port() -> int:
    import app

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    threading.Thread(target=server.serve_forever, daemon=True, name="redos-e2e").start()
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            break
        except OSError:
            time.sleep(0.05)
    yield port
    server.shutdown()
    server.server_close()


def _post_optimize(port: int, budget: str, ip: str) -> tuple[int, float]:
    import app

    token = app._generate_csrf_token()
    body = json.dumps({"budget": budget}).encode()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    t0 = time.perf_counter()
    conn.request(
        "POST",
        "/api/optimize",
        body=body,
        headers={
            "Content-Type": "application/json",
            "X-CSRF-Token": token,
            "Cookie": f"csrf_token={token}",
            "X-Forwarded-For": ip,
        },
    )
    resp = conn.getresponse()
    resp.read()
    return resp.status, time.perf_counter() - t0


def test_adversarial_post_to_a_real_endpoint_is_not_a_cpu_dos(live_port: int) -> None:
    """POST /api/optimize {"budget": "key " + "a"*80000}: the float() error is
    logged with the offending string, so the redaction filter sees it. Before the
    fix this took ~11 s here (quadratic); now it must stay near the benign cost."""
    import app  # noqa: F401  (module-level logging config installs the filter)

    status, benign = _post_optimize(live_port, "12345", "10.99.0.1")
    assert status in (200, 400, 500)
    status, hostile = _post_optimize(live_port, "key " + "a" * 80000, "10.99.0.2")
    assert status in (200, 400, 500)
    assert hostile < 2.0, f"hostile POST took {hostile:.2f}s (benign {benign:.3f}s)"
