"""Unit + multi-process tests for shared_state.py (the layered store).

The file layer is shared by gunicorn workers of one instance, so its
guarantees are cross-PROCESS guarantees and are tested with real separate
processes over one temp directory: visibility, TTL, caps, concurrent writers,
corrupt files. The durable layer is exercised over a real loopback HTTP
boundary (tests/fake_postgrest.py, which 409s a re-write without
``on_conflict=key`` exactly like prod's ``cache_key_key`` constraint) and, for
transport failures, by patching ``urllib.request.urlopen``.
"""

from __future__ import annotations

import io
import json
import os
import re
import secrets
import subprocess
import sys
import textwrap
import time
import urllib.error
from pathlib import Path
from typing import Any, Iterator

import pytest

import shared_state
from tests.fake_postgrest import FakePostgrest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_KEY_RE = re.compile(r"[A-Za-z0-9_-]{22}|[a-f0-9]{12}")


def _key() -> str:
    return secrets.token_urlsafe(16)


def _store(base: str, **overrides: Any) -> shared_state.SharedRecordStore:
    params: dict[str, Any] = dict(
        key_pattern=_KEY_RE,
        created_field="created",
        ttl_seconds=3600,
        max_record_bytes=4096,
        max_records=50,
        max_total_bytes=1024 * 1024,
        subdir="recs",
        durable=True,
    )
    params.update(overrides)
    return shared_state.SharedRecordStore("t", lambda: base, **params)


# A child process builds the same store over the same directory.
_CHILD_PRELUDE = """
import json, os, re, secrets, sys, time
sys.path.insert(0, {root!r})
import shared_state
KEY_RE = re.compile(r"[A-Za-z0-9_-]{{22}}|[a-f0-9]{{12}}")
store = shared_state.SharedRecordStore(
    "t", lambda: {base!r}, key_pattern=KEY_RE, created_field="created",
    ttl_seconds={ttl}, max_record_bytes={max_bytes}, max_records={max_records},
    max_total_bytes={max_total}, subdir="recs", durable=True)
"""


def _child(
    base: str,
    body: str,
    *,
    env: dict | None = None,
    ttl: float = 3600,
    max_bytes: int = 4096,
    max_records: int = 50,
    max_total: int = 1024 * 1024,
    wait: bool = True,
) -> Any:
    code = _CHILD_PRELUDE.format(
        root=str(PROJECT_ROOT),
        base=base,
        ttl=ttl,
        max_bytes=max_bytes,
        max_records=max_records,
        max_total=max_total,
    ) + textwrap.dedent(body)
    child_env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("SUPABASE_", "NOVA_STATE_", "NOVA_SHARED_STATE"))
    }
    child_env.update(env or {})
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=child_env,
    )
    if not wait:
        return proc
    out, err = proc.communicate(timeout=60)
    assert proc.returncode == 0, err
    return json.loads(out.strip().splitlines()[-1]) if out.strip() else None


def _files(base: str) -> list[Path]:
    d = Path(base) / "recs"
    return sorted(d.iterdir()) if d.is_dir() else []


@pytest.fixture()
def base(tmp_path: Path) -> str:
    return str(tmp_path / "instance")


@pytest.fixture()
def no_durable(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.delenv("NOVA_STATE_SUPABASE_URL", raising=False)
    monkeypatch.delenv("NOVA_STATE_SUPABASE_KEY", raising=False)
    monkeypatch.delenv("NOVA_SHARED_STATE_DURABLE", raising=False)


@pytest.fixture()
def fake(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakePostgrest]:
    server = FakePostgrest()
    monkeypatch.setenv("NOVA_SHARED_STATE_DURABLE", "1")  # opt-in, default off
    monkeypatch.setenv("NOVA_STATE_SUPABASE_URL", server.url)
    monkeypatch.setenv("NOVA_STATE_SUPABASE_KEY", "test-service-key")
    monkeypatch.setattr(shared_state, "_durable_down_until", 0.0)
    try:
        yield server
    finally:
        shared_state.flush(5)
        server.close()


# ---------------------------------------------------------------------------
# File layer: cross-process guarantees
# ---------------------------------------------------------------------------


def test_record_written_by_one_process_is_read_by_two_others(base: str, no_durable: None) -> None:
    key = _key()
    _child(base, f"print(json.dumps(store.put({key!r}, {{'created': time.time(), 'v': 'hello'}})))")
    for _ in range(2):
        got = _child(base, f"print(json.dumps(store.get({key!r})))")
        assert got and got["v"] == "hello"


def test_concurrent_writers_never_corrupt_and_caps_hold(base: str, no_durable: None) -> None:
    shared_keys = [_key() for _ in range(6)]
    writer = f"""
    shared = {shared_keys!r}
    for i in range(40):
        key = shared[i % len(shared)] if i % 2 else secrets.token_urlsafe(16)
        store.put(key, {{"created": time.time(), "pid": os.getpid(), "i": i, "pad": "x" * 300}})
    print(json.dumps(True))
    """
    reader = f"""
    shared = {shared_keys!r}
    bad = 0
    deadline = time.time() + 3
    while time.time() < deadline:
        for key in shared:
            rec = store.get(key)
            if rec is not None and not (isinstance(rec, dict) and rec.get("pad") == "x" * 300):
                bad += 1
    print(json.dumps(bad))
    """
    procs = [_child(base, writer, max_records=25, wait=False) for _ in range(4)]
    procs.append(_child(base, reader, max_records=25, wait=False))
    outs = []
    for p in procs:
        out, err = p.communicate(timeout=60)
        assert p.returncode == 0, err
        outs.append(json.loads(out.strip().splitlines()[-1]))
    assert outs[-1] == 0, "a reader saw a partial / corrupt record"

    files = _files(base)
    records = [f for f in files if f.suffix == ".json"]
    assert not [f for f in files if f.name.endswith(".tmp")], "orphaned temp files"
    assert len(records) <= 25
    for f in records:
        rec = json.loads(f.read_text())
        assert rec["pad"] == "x" * 300


def test_ttl_expiry_holds_across_processes(base: str, no_durable: None) -> None:
    stale, fresh = _key(), _key()
    _child(
        base,
        f"""
        store.put({stale!r}, {{"created": time.time() - 10, "v": 1}})
        store.put({fresh!r}, {{"created": time.time(), "v": 2}})
        print(json.dumps(True))
        """,
        ttl=5,
    )
    got = _child(base, f"print(json.dumps([store.get({stale!r}), store.get({fresh!r})]))", ttl=5)
    assert got[0] is None and got[1]["v"] == 2
    # The insert-time sweep already dropped the stale file...
    assert [f.name for f in _files(base) if f.suffix == ".json"] == [f"{fresh}.json"]
    # ...and a record that ages past its TTL on disk is swept by ANY process.
    past = time.time() - 10
    os.utime(Path(base, "recs", f"{fresh}.json"), (past, past))
    removed = _child(base, "print(json.dumps(store.sweep()))", ttl=5)
    assert removed == 1
    assert not [f for f in _files(base) if f.suffix == ".json"]


def test_record_size_cap_holds_on_the_file_layer(base: str, no_durable: None) -> None:
    store = _store(base, max_record_bytes=1024)
    big = _key()
    assert store.put(big, {"created": time.time(), "blob": "x" * 2000}) is False
    assert store.get(big) is None
    assert not _files(base)
    # A file over the cap that got onto disk anyway is never read.
    planted = _key()
    Path(base, "recs").mkdir(parents=True)
    Path(base, "recs", f"{planted}.json").write_text(
        json.dumps({"created": time.time(), "blob": "y" * 5000})
    )
    assert store.get(planted) is None


def test_count_and_byte_caps_evict_oldest_first(base: str, no_durable: None) -> None:
    store = _store(base, max_records=3, max_total_bytes=1024 * 1024)
    keys = [_key() for _ in range(5)]
    now = time.time()
    for i, key in enumerate(keys):
        assert store.put(key, {"created": now - 100 + i})
    assert {f.stem for f in _files(base) if f.suffix == ".json"} == set(keys[-3:])

    small = _store(str(Path(base) / "b2"), max_total_bytes=600)
    kept = []
    for i in range(6):
        key = _key()
        small.put(key, {"created": now - 50 + i, "pad": "z" * 150})
        kept.append(key)
    sizes = [f.stat().st_size for f in (Path(base) / "b2" / "recs").iterdir()]
    assert sum(sizes) <= 600
    assert small.get(kept[-1]) is not None and small.get(kept[0]) is None


def test_corrupt_partial_or_foreign_files_are_ignored_safely(base: str, no_durable: None) -> None:
    store = _store(base)
    good = _key()
    assert store.put(good, {"created": time.time(), "v": "ok"})
    recs = Path(base, "recs")
    cases = {
        _key(): '{"created": 17000',  # truncated JSON
        _key(): "\x00\x01garbage",
        _key(): json.dumps([1, 2, 3]),  # not an object
        _key(): json.dumps({"created": "yesterday", "v": 1}),  # no numeric creation time
    }
    for key, text in cases.items():
        (recs / f"{key}.json").write_text(text)
        assert store.get(key) is None
    assert store.get(good)["v"] == "ok"
    # Invalid ids never reach the filesystem at all.
    for bad in ("../../etc/passwd", "a/b", "", "x" * 23):
        assert store.get(bad) is None
        assert store.put(bad, {"created": time.time()}) is False


def test_sweep_removes_orphaned_temp_files_but_not_live_ones(base: str, no_durable: None) -> None:
    store = _store(base)
    assert store.put(_key(), {"created": time.time()})
    recs = Path(base, "recs")
    old_tmp = recs / f"{_key()}.json.999.dead.tmp"
    new_tmp = recs / f"{_key()}.json.999.live.tmp"
    old_tmp.write_text("{")
    new_tmp.write_text("{")
    past = time.time() - 3600
    os.utime(old_tmp, (past, past))
    store.sweep()
    assert not old_tmp.exists() and new_tmp.exists()


def test_sweep_never_touches_files_outside_its_prefix(base: str, no_durable: None) -> None:
    """Job records share the slot dir with the /api/generate lock files."""
    Path(base).mkdir(parents=True)
    foreign = [Path(base, n) for n in ("generate_slot_0.lock", "auto_qc_leader.lock", "other.json")]
    for f in foreign:
        f.write_text("x")
        os.utime(f, (1, 1))  # ancient: would be "expired" if it were considered
    jobs = _store(base, subdir="", prefix="job_", max_records=1)
    for _ in range(3):
        jobs.put(secrets.token_hex(6), {"created": time.time()})
    jobs.sweep()
    assert all(f.exists() for f in foreign)
    assert len(list(Path(base).glob("job_*.json"))) == 1


def test_kill_switch_turns_every_layer_off(
    base: str, fake: FakePostgrest, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NOVA_SHARED_STATE", "0")
    store = _store(base)
    key = _key()
    assert store.put(key, {"created": time.time()}) is False
    assert store.get(key) is None
    assert shared_state.flush(5)
    assert not _files(base) and fake.request_count() == 0


# ---------------------------------------------------------------------------
# Blob layer (plan ZIPs)
# ---------------------------------------------------------------------------


def test_blob_store_caps_and_ttl(base: str) -> None:
    blobs = shared_state.SharedBlobStore(
        "b",
        lambda: base,
        subdir="blobs",
        key_pattern=_KEY_RE,
        ttl_seconds=60,
        max_blob_bytes=1000,
        max_blobs=2,
        max_total_bytes=10_000,
    )
    now = time.time()
    k1, k2, k3, k_big, k_old = (_key() for _ in range(5))
    assert blobs.put(k_big, b"x" * 1001, now) is False  # per-blob cap
    assert blobs.put(k_old, b"old", now - 120) and blobs.get(k_old) is None  # TTL
    for i, key in enumerate((k1, k2, k3)):
        assert blobs.put(key, f"zip{i}".encode(), now - 10 + i)
    assert blobs.get(k1) is None  # count cap evicted the oldest
    assert blobs.get(k3) == b"zip2"
    got = _child(base, f"""
    blobs = shared_state.SharedBlobStore("b", lambda: {base!r}, subdir="blobs", key_pattern=KEY_RE,
        ttl_seconds=60, max_blob_bytes=1000, max_blobs=2, max_total_bytes=10_000)
    print(json.dumps((blobs.get({k2!r}) or b"").decode()))
    """)
    assert got == "zip1"


# ---------------------------------------------------------------------------
# Durable layer (Supabase `cache` table over a real loopback HTTP boundary)
# ---------------------------------------------------------------------------


def test_durable_round_trip_restores_state_on_an_empty_disk(
    tmp_path: Path, fake: FakePostgrest
) -> None:
    env = {
        "NOVA_SHARED_STATE_DURABLE": "1",
        "NOVA_STATE_SUPABASE_URL": fake.url,
        "NOVA_STATE_SUPABASE_KEY": "k",
    }
    key = _key()
    old_disk, new_disk = str(tmp_path / "old"), str(tmp_path / "new")
    _child(
        old_disk,
        f"""
        store.put({key!r}, {{"created": time.time(), "client": "Sealed Client Inc"}})
        store.put({key!r}, {{"created": time.time(), "client": "Sealed Client Inc", "rev": 2}})
        print(json.dumps(shared_state.flush(10)))
        """,
        env=env,
    )
    assert fake.request_count("POST") == 2  # the re-write upserted, no 409
    (row,) = fake.rows.values()
    dump = json.dumps(row)
    assert key not in dump and "Sealed Client Inc" not in dump
    assert row["key"].startswith("mpgstate:v1:t:") and row["category"] == "mpg_state"

    got = _child(new_disk, f"print(json.dumps(store.get({key!r})))", env=env)
    assert got == {"created": got["created"], "client": "Sealed Client Inc", "rev": 2}
    # ...and the new instance's file layer was backfilled.
    assert (Path(new_disk) / "recs" / f"{key}.json").exists()


def test_tampered_or_wrong_key_rows_are_rejected(base: str, fake: FakePostgrest) -> None:
    store = _store(base)
    key = _key()
    assert store.put(key, {"created": time.time(), "v": 1})
    assert shared_state.flush(5)
    (row,) = fake.rows.values()
    row["data"]["c"] = row["data"]["c"][::-1]
    other = _store(str(Path(base) / "fresh"))
    assert other.get(key) is None


def test_short_legacy_ids_are_never_written_durably(base: str, fake: FakePostgrest) -> None:
    store = _store(base)
    assert store.put(secrets.token_hex(6), {"created": time.time()})
    assert shared_state.flush(5)
    assert fake.request_count("POST") == 0


def test_expired_records_are_not_written_or_served_durably(base: str, fake: FakePostgrest) -> None:
    store = _store(base, ttl_seconds=5)
    key = _key()
    store.put(key, {"created": time.time() - 60})
    assert shared_state.flush(5)
    assert fake.request_count("POST") == 0
    assert _store(str(Path(base) / "fresh"), ttl_seconds=5).get(key) is None


def test_durable_size_cap_matches_the_file_cap(base: str, fake: FakePostgrest) -> None:
    store = _store(base, max_record_bytes=1024)
    assert store.put(_key(), {"created": time.time(), "blob": "x" * 5000}) is False
    assert shared_state.flush(5)
    assert fake.request_count("POST") == 0


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://x", code, "err", {}, io.BytesIO(b""))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "make_failure,backoff",
    [
        (lambda: urllib.error.URLError("connection refused"), shared_state._DURABLE_BACKOFF_SECONDS),
        (lambda: TimeoutError("read timed out"), shared_state._DURABLE_BACKOFF_SECONDS),
        (lambda: _http_error(503), shared_state._DURABLE_BACKOFF_SECONDS),
        (lambda: _http_error(401), shared_state._DURABLE_CONFIG_BACKOFF_SECONDS),
    ],
    ids=["url-error", "timeout", "http-503", "http-401"],
)
def test_durable_failures_fall_through_and_back_off(
    base: str,
    fake: FakePostgrest,
    monkeypatch: pytest.MonkeyPatch,
    make_failure: Any,
    backoff: float,
) -> None:
    calls: list[str] = []

    def _failing_urlopen(req: Any, *args: Any, **kwargs: Any) -> Any:
        calls.append(req.get_method())
        raise make_failure()

    monkeypatch.setattr(shared_state.urllib.request, "urlopen", _failing_urlopen)
    store = _store(base)
    key = _key()
    assert store.put(key, {"created": time.time(), "v": "file"}) is True
    assert shared_state.flush(5)
    assert calls == ["POST"]
    assert store.get(key) == {"created": store.get(key)["created"], "v": "file"}
    down_for = shared_state._durable_down_until - time.time()
    assert backoff - 5 < down_for <= backoff
    # While backed off, a file miss costs no network call and raises nothing.
    assert _store(str(Path(base) / "fresh")).get(_key()) is None
    assert calls == ["POST"]


def test_durable_get_failure_on_file_miss_returns_none(
    base: str, fake: FakePostgrest
) -> None:
    fake.fail_with = 500
    assert _store(base).get(_key()) is None
    assert shared_state._durable_down_until > time.time()


# ---------------------------------------------------------------------------
# Sealing: AES-256-GCM when `cryptography` imports, stdlib fallback otherwise.
# Both paths run here: the stdlib one by making the import fail; the AES-GCM
# one wherever `cryptography` is installed (prod; skipped on a bare dev box).
# ---------------------------------------------------------------------------

_AEAD_MODULE = "cryptography.hazmat.primitives.ciphers.aead"
_SECRET = "server-only-secret"


@pytest.fixture(params=["stdlib", "aesgcm"])
def seal_path(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> int:
    """The envelope version the active path writes."""
    if request.param == "stdlib":
        monkeypatch.setitem(sys.modules, _AEAD_MODULE, None)  # import -> ImportError
        assert shared_state._aesgcm_class() is None
        return shared_state._ENVELOPE_STDLIB
    pytest.importorskip(_AEAD_MODULE)
    assert shared_state._aesgcm_class() is not None
    return shared_state._ENVELOPE_AESGCM


def _xor(a: bytes, b: bytes) -> bytes:
    assert len(a) == len(b)
    return bytes(a[i] ^ b[i] for i in range(len(a)))


def test_seal_round_trip_on_each_path(seal_path: int) -> None:
    key = secrets.token_urlsafe(16)
    env = shared_state.seal(_SECRET, key, "share", b'{"a": 1}')
    assert env["v"] == seal_path
    assert shared_state.unseal(_SECRET, key, "share", env) == b'{"a": 1}'
    empty = shared_state.seal(_SECRET, key, "share", b"")
    assert shared_state.unseal(_SECRET, key, "share", empty) == b""


def test_resealing_the_same_id_never_reuses_a_keystream(seal_path: int) -> None:
    """A job record is re-sealed on completion and again on qa-ack. If the
    keystream depended only on (secret, id, namespace), the XOR of the two
    ciphertexts would equal the XOR of the two plaintexts (a two-time pad).
    A fresh per-seal nonce must break that relation."""
    key = secrets.token_urlsafe(16)
    p1 = b'{"status":"processing","acked_by":"","pct":40}'
    p2 = p1.replace(b"processing", b"completed!").replace(b'"pct":40', b'"pct":99')
    assert len(p1) == len(p2) and p1 != p2
    e1 = shared_state.seal(_SECRET, key, "job", p1)
    e2 = shared_state.seal(_SECRET, key, "job", p2)
    e1_again = shared_state.seal(_SECRET, key, "job", p1)
    assert len({e1["n"], e2["n"], e1_again["n"]}) == 3, "nonce reused"
    c1 = shared_state._unb64(e1["c"])[: len(p1)]
    c2 = shared_state._unb64(e2["c"])[: len(p2)]
    assert _xor(c1, c2) != _xor(p1, p2), "keystream reused across re-seals"
    assert shared_state._unb64(e1_again["c"])[: len(p1)] != c1
    assert shared_state.unseal(_SECRET, key, "job", e1) == p1
    assert shared_state.unseal(_SECRET, key, "job", e2) == p2


def test_tampered_forged_or_replayed_envelopes_are_rejected(seal_path: int) -> None:
    key, other = secrets.token_urlsafe(16), secrets.token_urlsafe(16)
    env = shared_state.seal(_SECRET, key, "share", b'{"client": "Acme"}')
    other_env = shared_state.seal(_SECRET, other, "share", b'{"client": "Other"}')
    body = bytearray(shared_state._unb64(env["c"]))
    body[0] ^= 1
    for label, candidate in {
        "public anon key": ("public-anon-key", key, "share", env),
        "replayed under another id": (_SECRET, other, "share", env),
        "replayed under another namespace": (_SECRET, key, "job", env),
        "flipped ciphertext bit": (_SECRET, key, "share", {**env, "c": shared_state._b64(bytes(body))}),
        "nonce from another envelope": (_SECRET, key, "share", {**env, "n": other_env["n"]}),
        "unknown version": (_SECRET, key, "share", {**env, "v": 99}),
        "bad base64": (_SECRET, key, "share", {**env, "n": "!!"}),
        "not a dict": (_SECRET, key, "share", "not-a-dict"),
    }.items():
        assert shared_state.unseal(*candidate) is None, label


def test_aesgcm_envelope_on_a_host_without_cryptography_is_a_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, _AEAD_MODULE, None)
    env = {
        "v": shared_state._ENVELOPE_AESGCM,
        "n": shared_state._b64(secrets.token_bytes(12)),
        "c": shared_state._b64(secrets.token_bytes(48)),
    }
    assert shared_state.unseal(_SECRET, _key(), "share", env) is None


def test_stdlib_envelopes_stay_readable_where_aesgcm_is_preferred(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip(_AEAD_MODULE)
    key = _key()
    with monkeypatch.context() as m:
        m.setitem(sys.modules, _AEAD_MODULE, None)
        env = shared_state.seal(_SECRET, key, "share", b"legacy")
    assert env["v"] == shared_state._ENVELOPE_STDLIB
    assert shared_state.unseal(_SECRET, key, "share", env) == b"legacy"


# ---------------------------------------------------------------------------
# The durable layer is OPT-IN
# ---------------------------------------------------------------------------


def test_durable_layer_is_off_unless_explicitly_opted_in(
    base: str, fake: FakePostgrest, monkeypatch: pytest.MonkeyPatch
) -> None:
    for value in (None, "", "0", "true", "yes"):
        if value is None:
            monkeypatch.delenv("NOVA_SHARED_STATE_DURABLE", raising=False)
        else:
            monkeypatch.setenv("NOVA_SHARED_STATE_DURABLE", value)
        assert shared_state.durable_configured() is False, value
        assert "durable layer OFF (opt-in: NOVA_SHARED_STATE_DURABLE=1)" in (
            shared_state.startup_summary()
        )
        store = _store(base)
        key = _key()
        assert store.put(key, {"created": time.time()})  # file layer still works
        assert shared_state.flush(5)
        assert _store(str(Path(base) / "fresh")).get(key) is None
    assert fake.request_count() == 0
    monkeypatch.setenv("NOVA_SHARED_STATE_DURABLE", "1")
    assert shared_state.durable_configured() is True
    assert "durable layer ON" in shared_state.startup_summary()
    monkeypatch.setenv("NOVA_SHARED_STATE", "0")
    assert "all layers OFF" in shared_state.startup_summary()


def test_rows_forged_with_the_public_key_or_moved_between_ids_are_rejected(
    base: str, fake: FakePostgrest
) -> None:
    """The cache table is writable with the public anon key: a planted row
    must never be served, and a real row copied under another id's row key
    must not authenticate either."""
    reader = _store(str(Path(base) / "fresh"))
    server_key = "test-service-key"
    planted_id = _key()
    planted_row_key = shared_state._row_key(server_key, "t", planted_id)
    fake.rows[planted_row_key] = {
        "key": planted_row_key,
        "data": shared_state.seal(
            "public-anon-key", planted_id, "t", json.dumps({"created": time.time()}).encode()
        ),
    }
    assert reader.get(planted_id) is None

    real_id, other_id = _key(), _key()
    assert _store(base).put(real_id, {"created": time.time(), "v": "real"})
    assert shared_state.flush(5)
    real_row = fake.rows[shared_state._row_key(server_key, "t", real_id)]
    other_row_key = shared_state._row_key(server_key, "t", other_id)
    fake.rows[other_row_key] = {**real_row, "key": other_row_key}
    assert reader.get(other_id) is None
    assert reader.get(real_id)["v"] == "real"


def test_anon_key_alone_never_enables_the_durable_layer(
    base: str, fake: FakePostgrest, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("NOVA_STATE_SUPABASE_KEY")
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.setenv("SUPABASE_ANON_KEY", "public-anon-key")
    assert shared_state.durable_configured() is False
    store = _store(base)
    key = _key()
    assert store.put(key, {"created": time.time()})
    assert shared_state.flush(5)
    assert _store(str(Path(base) / "fresh")).get(key) is None
    assert fake.request_count() == 0
