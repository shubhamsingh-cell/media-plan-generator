"""Cross-worker, restart-surviving storage for small JSON state records.

Why this exists
---------------
Production runs ``gunicorn --worker-class gevent --workers 4 --preload`` on
ONE Render instance with no persistent disk (Render service API, read
2026-10-01). Share links, plan results, generation-job status and the QA
override-ack lived only in per-process dicts, so a request that landed on a
worker other than the writer 404'd (``qa-ack``: 3 of 5 real prod jobs), and
every deploy wiped them all.

Layers (app.py keeps its in-process dicts as the fast path; this module is
everything below them). Reads go file -> durable and backfill upward; writes
go to every layer. Every layer is fail-soft: an error logs once and the call
falls through -- nothing here raises into a request handler.

* **File layer** -- one file per key under an instance-local directory that
  all workers of the instance share. Writes are atomic (temp file +
  ``os.replace``), so a reader never sees a partial record; the file's mtime is
  set to the record's creation time, so TTL and oldest-first eviction need no
  parsing. Sweeps (TTL + count/byte caps + stale temp files) are serialised
  across processes with ``fcntl.flock`` (non-blocking + retry: a blocking flock
  would stall a gevent worker's whole event loop). Render's disk is wiped on
  deploy, so this layer alone does not survive restarts.
* **Durable layer** -- the Supabase ``cache`` table that already exists in prod
  (supabase_cache.py writes it; its 409 ``cache_key_key`` errors in the
  2026-10-01 telemetry prove the table, its columns, and that ``key`` is a
  UNIQUE non-primary column). No schema change: rows are upserted with
  ``?on_conflict=key`` so re-writing a key updates in place instead of 409ing.
  Writes are queued to one background thread per process (never on the request
  path); a transport failure backs the layer off for 30 s, a configuration
  failure (401/403/404) for 10 min.

The durable rows are sealed
---------------------------
The anon key is public (``GET /api/config`` serves it to every browser for
Supabase Auth) and the repo's DDL gives ``cache`` an allow-all RLS policy, so
anyone can READ and WRITE that table. Each id stored here (share id, job id,
plan id) is a >=128-bit bearer secret, and the records are client plans, so:

* the row key is an HMAC of the id -- listing the table reveals no id;
* the payload is encrypted and authenticated (encrypt-then-MAC: SHAKE-256
  keystream, HMAC-SHA256 tag over nonce + ciphertext) under keys derived from
  a SERVER-ONLY secret and the id. Readers of the table learn nothing, and a
  forged or swapped row fails authentication and is treated as a miss --
  without that, anyone could plant a "plan result" and have /plan/<id> render
  it from our origin.
* the server-only secret is the service-role key the layer authenticates
  with (or ``NOVA_STATE_SUPABASE_KEY``). With only the public anon key
  configured the layer stays OFF (fail-closed) and says so once in the log.

Ids shorter than 22 characters (legacy 8-hex share ids, 12-hex job ids) are
never written durably.

Kill switches (environment, read at call time): ``NOVA_SHARED_STATE=0`` turns
both layers off (exact pre-change behaviour: in-process dicts only);
``NOVA_SHARED_STATE_DURABLE=0`` turns off only the Supabase layer.
``NOVA_STATE_SUPABASE_URL`` / ``NOVA_STATE_SUPABASE_KEY`` override the
Supabase target (tests point them at a loopback fake); otherwise
``SUPABASE_URL`` with ``SUPABASE_SERVICE_ROLE_KEY`` is used.

Stdlib only.
"""

from __future__ import annotations

import atexit
import base64
import binascii
import contextlib
import hashlib
import hmac
import http.client
import json
import logging
import os
import queue
import re
import secrets
import ssl
import stat
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Optional

try:
    import fcntl
except ImportError:  # non-POSIX dev box: single process, no locking needed
    fcntl = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# ── File layer ──────────────────────────────────────────────────────────────
_RECORD_SUFFIX = ".json"
_BLOB_SUFFIX = ".bin"
_TMP_SUFFIX = ".tmp"
_TMP_GRACE_SECONDS = 120.0  # a writer's temp file older than this was orphaned
_LOCK_WAIT_SECONDS = 2.0
_LOCK_POLL_SECONDS = 0.01
_DIR_MODE = 0o700
_FILE_MODE = 0o600
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)

# ── Durable layer ───────────────────────────────────────────────────────────
_DURABLE_TABLE = "cache"
_DURABLE_CATEGORY = "mpg_state"
_DURABLE_TIMEOUT_SECONDS = 3.0
_DURABLE_BACKOFF_SECONDS = 30.0
_DURABLE_CONFIG_BACKOFF_SECONDS = 600.0
_DURABLE_MIN_KEY_LEN = 22
_DURABLE_QUEUE_MAX = 256
_DURABLE_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
_SEAL_VERSION = 1
_NONCE_BYTES = 16
_TAG_BYTES = 32



def layers_enabled() -> bool:
    """False only under ``NOVA_SHARED_STATE=0`` (in-process dicts only)."""
    return (os.environ.get("NOVA_SHARED_STATE") or "1").strip() != "0"


# ═══════════════════════════════════════════════════════════════════════════
# Logging: every layer error is logged once, never with payload contents
# ═══════════════════════════════════════════════════════════════════════════
_logged_signatures: set[str] = set()
_logged_lock = threading.Lock()
_LOGGED_SIGNATURES_MAX = 256


def _log_once(signature: str, level: int, message: str) -> None:
    with _logged_lock:
        if signature in _logged_signatures:
            return
        if len(_logged_signatures) < _LOGGED_SIGNATURES_MAX:
            _logged_signatures.add(signature)
    logger.log(level, message)


def _created_of(record: Any, field: str) -> Optional[float]:
    """The record's creation timestamp, or None when missing / not a number."""
    if not isinstance(record, dict):
        return None
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


# ═══════════════════════════════════════════════════════════════════════════
# File layer
# ═══════════════════════════════════════════════════════════════════════════


class _FileDir:
    """A directory of one-file-per-key entries shared by every worker process.

    ``base_dir`` is a callable so the directory resolves at call time (app.py's
    slot directory is monkeypatched per test and read lazily there too).
    Only files named ``<prefix><key><suffix>`` (and their temp files) are ever
    touched, so a store may share a directory with unrelated files -- the job
    records live beside the /api/generate slot lock files.
    """

    def __init__(
        self,
        name: str,
        base_dir: Callable[[], str],
        *,
        subdir: str,
        prefix: str,
        suffix: str,
        key_pattern: "re.Pattern[str]",
        ttl_seconds: float,
        max_entry_bytes: int,
        max_entries: int,
        max_total_bytes: int,
    ) -> None:
        self.name = name
        self._base_dir = base_dir
        self._subdir = subdir
        self._prefix = prefix
        self._suffix = suffix
        self._key_pattern = key_pattern
        self.ttl_seconds = float(ttl_seconds)
        self.max_entry_bytes = int(max_entry_bytes)
        self.max_entries = int(max_entries)
        self.max_total_bytes = int(max_total_bytes)
        self._lock_name = f".{prefix}sweep.lock"

    def directory(self) -> str:
        base = self._base_dir()
        return os.path.join(base, self._subdir) if self._subdir else base

    def path(self, key: str) -> Optional[str]:
        """File path for ``key``, or None when ``key`` is not a valid id.

        The key pattern is the path-traversal guard: nothing that is not a
        plain id ever reaches os.path.join.
        """
        if not isinstance(key, str) or not self._key_pattern.fullmatch(key):
            return None
        return os.path.join(self.directory(), f"{self._prefix}{key}{self._suffix}")

    def read(self, path: str) -> Optional[bytes]:
        """Bytes of ``path`` if it is a regular file within the size cap."""
        try:
            fd = os.open(path, os.O_RDONLY | _O_NOFOLLOW)
        except FileNotFoundError:
            return None
        except OSError as exc:
            _log_once(
                f"file-read:{self.name}:{exc.errno}",
                logging.WARNING,
                f"shared_state[{self.name}]: file read failed ({exc.__class__.__name__}, "
                f"errno {exc.errno}); falling through",
            )
            return None
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_size > self.max_entry_bytes:
                return None
            with os.fdopen(fd, "rb") as fh:
                fd = -1
                return fh.read(self.max_entry_bytes + 1)
        except OSError:
            return None
        finally:
            if fd >= 0:
                os.close(fd)

    def mtime(self, path: str) -> Optional[float]:
        try:
            st = os.lstat(path)
        except OSError:
            return None
        return st.st_mtime if stat.S_ISREG(st.st_mode) else None

    def write(self, path: str, data: bytes, created: float) -> bool:
        """Atomically replace ``path`` with ``data``; mtime := ``created``."""
        tmp = f"{path}.{os.getpid()}.{secrets.token_hex(4)}{_TMP_SUFFIX}"
        try:
            os.makedirs(os.path.dirname(path), mode=_DIR_MODE, exist_ok=True)
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW, _FILE_MODE)
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            os.utime(tmp, (created, created))
            os.replace(tmp, path)
            return True
        except OSError as exc:
            _log_once(
                f"file-write:{self.name}:{exc.errno}",
                logging.WARNING,
                f"shared_state[{self.name}]: file write failed ({exc.__class__.__name__}, "
                f"errno {exc.errno}); this worker keeps the entry in memory only",
            )
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            return False

    def unlink(self, path: str) -> None:
        with contextlib.suppress(OSError):
            os.unlink(path)

    @contextlib.contextmanager
    def _sweep_lock(self, directory: str) -> Iterator[bool]:
        """Cross-process exclusive lock for a sweep; yields False if not taken."""
        if fcntl is None:
            yield True
            return
        try:
            os.makedirs(directory, mode=_DIR_MODE, exist_ok=True)
            fd = os.open(
                os.path.join(directory, self._lock_name), os.O_RDWR | os.O_CREAT, _FILE_MODE
            )
        except OSError:
            yield False
            return
        acquired = False
        try:
            deadline = time.monotonic() + _LOCK_WAIT_SECONDS
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(_LOCK_POLL_SECONDS)
                except OSError:
                    break
            yield acquired
        finally:
            if acquired:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def sweep(self, now: Optional[float] = None) -> int:
        """Drop expired entries and orphaned temp files, then evict oldest-first
        until the count and byte caps hold. Returns the number of files removed."""
        now = time.time() if now is None else now
        directory = self.directory()
        if not os.path.isdir(directory):
            return 0
        removed = 0
        with self._sweep_lock(directory) as locked:
            if not locked:
                return 0
            try:
                names = os.listdir(directory)
            except OSError:
                return 0
            live: list[tuple[float, int, str]] = []
            for fname in names:
                if not fname.startswith(self._prefix):
                    continue
                fpath = os.path.join(directory, fname)
                try:
                    st = os.lstat(fpath)
                except OSError:
                    continue
                if not stat.S_ISREG(st.st_mode):
                    continue
                if fname.endswith(_TMP_SUFFIX):
                    if now - st.st_mtime > _TMP_GRACE_SECONDS:
                        self.unlink(fpath)
                        removed += 1
                    continue
                if not fname.endswith(self._suffix):
                    continue
                if now - st.st_mtime > self.ttl_seconds:
                    self.unlink(fpath)
                    removed += 1
                    continue
                live.append((st.st_mtime, st.st_size, fpath))
            live.sort()
            count = len(live)
            total = sum(size for _m, size, _p in live)
            for _mtime, size, fpath in live:
                if count <= self.max_entries and total <= self.max_total_bytes:
                    break
                self.unlink(fpath)
                count -= 1
                total -= size
                removed += 1
        return removed


# ═══════════════════════════════════════════════════════════════════════════
# Durable layer: Supabase ``cache`` table over PostgREST (urllib only)
# ═══════════════════════════════════════════════════════════════════════════
_durable_state_lock = threading.Lock()
_durable_down_until = 0.0
_ssl_context: Optional[ssl.SSLContext] = None


def _durable_config() -> tuple[str, str]:
    """(base_url, server_key) of the durable layer, or ("", "") when it is off.

    The key must be server-only: it authenticates the PostgREST calls AND is
    the secret rows are sealed under. The anon key is public (``/api/config``
    serves it), so it is never used here -- anon-only means the layer is off.
    """
    if (os.environ.get("NOVA_SHARED_STATE_DURABLE") or "1").strip() == "0":
        return "", ""
    url = (
        os.environ.get("NOVA_STATE_SUPABASE_URL") or os.environ.get("SUPABASE_URL") or ""
    ).strip()
    key = (
        os.environ.get("NOVA_STATE_SUPABASE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or ""
    ).strip()
    if url and not key and (os.environ.get("SUPABASE_ANON_KEY") or "").strip():
        _log_once(
            "durable-anon-only",
            logging.WARNING,
            "shared_state durable layer OFF: only the public SUPABASE_ANON_KEY is set; "
            "restart-surviving state needs SUPABASE_SERVICE_ROLE_KEY (server-only)",
        )
    if not url or not key:
        return "", ""
    return url.rstrip("/"), key


def durable_configured() -> bool:
    return layers_enabled() and bool(_durable_config()[0])


def _durable_ready() -> bool:
    if not durable_configured():
        return False
    with _durable_state_lock:
        return time.time() >= _durable_down_until


def _mark_durable_down(seconds: float, signature: str, message: str) -> None:
    """Back the durable layer off; log once per outage episode."""
    global _durable_down_until
    now = time.time()
    with _durable_state_lock:
        was_up = now >= _durable_down_until
        _durable_down_until = max(_durable_down_until, now + seconds)
    if was_up:
        level = logging.ERROR if seconds >= _DURABLE_CONFIG_BACKOFF_SECONDS else logging.WARNING
        # Episode-scoped: a new outage after recovery logs again.
        logger.log(level, f"shared_state durable layer paused {seconds:.0f}s: {message} [{signature}]")


def _get_ssl_context() -> ssl.SSLContext:
    global _ssl_context
    if _ssl_context is None:
        _ssl_context = ssl.create_default_context()
    return _ssl_context


def _durable_request(
    method: str, path_and_query: str, body: Optional[bytes] = None, prefer: str = ""
) -> tuple[int, bytes]:
    """One PostgREST call. Returns (status, body); status 0 = transport failure.

    Never raises for network/HTTP errors. Logs status codes and exception
    classes only -- never URLs (they carry row keys) or payloads.
    """
    base, api_key = _durable_config()
    if not base:
        return 0, b""
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    req = urllib.request.Request(
        f"{base}/rest/v1/{path_and_query}", data=body, method=method, headers=headers
    )
    context = _get_ssl_context() if base.startswith("https://") else None
    try:
        with urllib.request.urlopen(req, timeout=_DURABLE_TIMEOUT_SECONDS, context=context) as resp:
            return resp.status, resp.read(_DURABLE_MAX_RESPONSE_BYTES)
    except urllib.error.HTTPError as exc:
        status = exc.code
        if status >= 500:
            _mark_durable_down(_DURABLE_BACKOFF_SECONDS, f"http-{status}", f"HTTP {status} on {method}")
        elif status in (401, 403, 404):
            _mark_durable_down(
                _DURABLE_CONFIG_BACKOFF_SECONDS,
                f"http-{status}",
                f"HTTP {status} on {method} (key or table misconfigured)",
            )
        else:
            _log_once(
                f"durable-http-{method}-{status}",
                logging.WARNING,
                f"shared_state durable layer: HTTP {status} on {method}; entry kept in the file layer only",
            )
        return status, b""
    except (urllib.error.URLError, OSError, http.client.HTTPException, ValueError) as exc:
        _mark_durable_down(
            _DURABLE_BACKOFF_SECONDS, exc.__class__.__name__, f"{exc.__class__.__name__} on {method}"
        )
        return 0, b""


def _derive(secret: str, key: str, namespace: str, purpose: str) -> bytes:
    """Per-record key material bound to the server secret, the id and the
    namespace -- so a row can be neither read, forged, nor moved to another id."""
    return hmac.new(
        secret.encode("utf-8"),
        f"mpgstate/v{_SEAL_VERSION}/{purpose}/{namespace}/{key}".encode("utf-8"),
        hashlib.sha256,
    ).digest()


def _row_key(secret: str, namespace: str, key: str) -> str:
    return f"mpgstate:v{_SEAL_VERSION}:{namespace}:{_derive(secret, key, namespace, 'row').hex()}"


def _xor_keystream(data: bytes, enc_key: bytes, nonce: bytes) -> bytes:
    if not data:
        return b""
    stream = hashlib.shake_256(enc_key + nonce).digest(len(data))
    return (int.from_bytes(data, "big") ^ int.from_bytes(stream, "big")).to_bytes(len(data), "big")


def seal(secret: str, key: str, namespace: str, plaintext: bytes) -> dict:
    """Encrypt-then-MAC ``plaintext`` for record ``key`` of ``namespace``."""
    nonce = secrets.token_bytes(_NONCE_BYTES)
    ciphertext = _xor_keystream(plaintext, _derive(secret, key, namespace, "enc"), nonce)
    tag = hmac.new(
        _derive(secret, key, namespace, "mac"), nonce + ciphertext, hashlib.sha256
    ).digest()
    return {
        "v": _SEAL_VERSION,
        "n": base64.b64encode(nonce).decode("ascii"),
        "c": base64.b64encode(ciphertext).decode("ascii"),
        "t": base64.b64encode(tag).decode("ascii"),
    }


def unseal(secret: str, key: str, namespace: str, envelope: Any) -> Optional[bytes]:
    """Plaintext of a :func:`seal` envelope, or None if malformed or forged."""
    if not isinstance(envelope, dict) or envelope.get("v") != _SEAL_VERSION:
        return None
    try:
        nonce = base64.b64decode(str(envelope.get("n") or ""), validate=True)
        ciphertext = base64.b64decode(str(envelope.get("c") or ""), validate=True)
        tag = base64.b64decode(str(envelope.get("t") or ""), validate=True)
    except (binascii.Error, ValueError):
        return None
    if len(nonce) != _NONCE_BYTES or len(tag) != _TAG_BYTES:
        return None
    expected = hmac.new(
        _derive(secret, key, namespace, "mac"), nonce + ciphertext, hashlib.sha256
    ).digest()
    if not hmac.compare_digest(expected, tag):
        return None
    return _xor_keystream(ciphertext, _derive(secret, key, namespace, "enc"), nonce)


class _DurableWriter:
    """One daemon writer thread per process, fed by a bounded queue.

    Durable writes never run on a request thread. Created lazily and per pid:
    gunicorn ``--preload`` forks workers after import, and a queue/thread
    inherited from the master would be orphaned in the child.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._queue: Optional["queue.Queue[tuple[Callable[..., Any], tuple]]"] = None
        self._pid = 0

    def _ensure(self) -> "queue.Queue[tuple[Callable[..., Any], tuple]]":
        with self._lock:
            if self._queue is None or self._pid != os.getpid():
                self._queue = queue.Queue(maxsize=_DURABLE_QUEUE_MAX)
                self._pid = os.getpid()
                threading.Thread(
                    target=self._run, args=(self._queue,), daemon=True, name="shared-state-durable"
                ).start()
            return self._queue

    def submit(self, fn: Callable[..., Any], *args: Any) -> bool:
        try:
            self._ensure().put_nowait((fn, args))
            return True
        except queue.Full:
            _log_once(
                "durable-queue-full",
                logging.WARNING,
                "shared_state durable queue full; dropping a durable write (file layer still has it)",
            )
            return False

    @staticmethod
    def _run(q: "queue.Queue[tuple[Callable[..., Any], tuple]]") -> None:
        while True:
            fn, args = q.get()
            try:
                fn(*args)
            except Exception as exc:  # error isolation: one bad write never kills the writer
                logger.error(f"shared_state durable write failed: {exc.__class__.__name__}", exc_info=True)
            finally:
                q.task_done()

    def flush(self, timeout: float) -> bool:
        """Wait until every queued durable write has been attempted."""
        with self._lock:
            q = self._queue if self._pid == os.getpid() else None
        if q is None:
            return True
        deadline = time.monotonic() + timeout
        while q.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.02)
        return not q.unfinished_tasks


_writer = _DurableWriter()


def flush(timeout: float = 5.0) -> bool:
    """Block until queued durable writes are done (tests, graceful shutdown)."""
    return _writer.flush(timeout)


atexit.register(flush, 2.0)


class _DurableTable:
    """Sealed records for one namespace in the shared ``cache`` table."""

    def __init__(self, namespace: str, ttl_seconds: float, max_record_bytes: int) -> None:
        self.namespace = namespace
        self._ttl = float(ttl_seconds)
        self._max = int(max_record_bytes)

    def eligible(self, key: str) -> bool:
        return len(key) >= _DURABLE_MIN_KEY_LEN

    def get(self, key: str) -> Optional[dict]:
        if not self.eligible(key) or not _durable_ready():
            return None
        secret = _durable_config()[1]
        row_key = urllib.parse.quote(_row_key(secret, self.namespace, key), safe="")
        status, body = _durable_request(
            "GET", f"{_DURABLE_TABLE}?key=eq.{row_key}&select=data&limit=1"
        )
        if status != 200 or not body:
            return None
        try:
            rows = json.loads(body)
        except ValueError:
            return None
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            return None
        plaintext = unseal(secret, key, self.namespace, rows[0].get("data"))
        if plaintext is None:
            _log_once(
                f"durable-unseal-{self.namespace}",
                logging.WARNING,
                f"shared_state[{self.namespace}]: a durable row failed authentication; ignored",
            )
            return None
        if len(plaintext) > self._max:
            return None
        try:
            record = json.loads(plaintext)
        except ValueError:
            return None
        return record if isinstance(record, dict) else None

    def put_now(self, key: str, data: bytes, created: float) -> bool:
        expires = created + self._ttl
        if not self.eligible(key) or expires <= time.time() or not _durable_ready():
            return False
        secret = _durable_config()[1]
        row = {
            "key": _row_key(secret, self.namespace, key),
            "data": seal(secret, key, self.namespace, data),
            "created_at": datetime.fromtimestamp(created, timezone.utc).isoformat(),
            "expires_at": datetime.fromtimestamp(expires, timezone.utc).isoformat(),
            "category": _DURABLE_CATEGORY,
        }
        status, _body = _durable_request(
            "POST",
            f"{_DURABLE_TABLE}?on_conflict=key",
            body=json.dumps(row, separators=(",", ":")).encode("utf-8"),
            prefer="resolution=merge-duplicates,return=minimal",
        )
        return 200 <= status < 300

    def put_async(self, key: str, data: bytes, created: float) -> bool:
        if not self.eligible(key) or not durable_configured():
            return False
        return _writer.submit(self.put_now, key, data, created)


# ═══════════════════════════════════════════════════════════════════════════
# Public stores
# ═══════════════════════════════════════════════════════════════════════════


class SharedRecordStore:
    """File + durable layers for JSON-object records keyed by an id.

    A record carries its own creation time in ``created_field``; TTL and
    eviction order are derived from it on every layer. Records larger than
    ``max_record_bytes`` (serialized) are refused by every layer.
    """

    def __init__(
        self,
        name: str,
        base_dir: Callable[[], str],
        *,
        key_pattern: "re.Pattern[str]",
        created_field: str,
        ttl_seconds: float,
        max_record_bytes: int,
        max_records: int,
        max_total_bytes: int,
        subdir: str = "",
        prefix: str = "",
        durable: bool = True,
    ) -> None:
        self.name = name
        self._created_field = created_field
        self._files = _FileDir(
            name,
            base_dir,
            subdir=subdir,
            prefix=prefix,
            suffix=_RECORD_SUFFIX,
            key_pattern=key_pattern,
            ttl_seconds=ttl_seconds,
            max_entry_bytes=max_record_bytes,
            max_entries=max_records,
            max_total_bytes=max_total_bytes,
        )
        self._durable = (
            _DurableTable(name, ttl_seconds, max_record_bytes) if durable else None
        )

    @property
    def ttl_seconds(self) -> float:
        return self._files.ttl_seconds

    def _fresh(self, record: dict, now: float) -> bool:
        created = _created_of(record, self._created_field)
        return created is not None and now - created <= self._files.ttl_seconds

    def _encode(self, key: str, record: Any) -> Optional[tuple[bytes, float]]:
        created = _created_of(record, self._created_field)
        if created is None:
            _log_once(
                f"no-created-{self.name}",
                logging.WARNING,
                f"shared_state[{self.name}]: record without a numeric {self._created_field!r}; not persisted",
            )
            return None
        try:
            data = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            _log_once(
                f"encode-{self.name}",
                logging.WARNING,
                f"shared_state[{self.name}]: record not JSON-serializable ({exc.__class__.__name__}); not persisted",
            )
            return None
        if len(data) > self._files.max_entry_bytes:
            _log_once(
                f"too-big-{self.name}",
                logging.INFO,
                f"shared_state[{self.name}]: record over the {self._files.max_entry_bytes}-byte cap; "
                "kept in this worker's memory only",
            )
            return None
        return data, created

    @staticmethod
    def _decode(raw: Optional[bytes]) -> Optional[dict]:
        if raw is None:
            return None
        try:
            record = json.loads(raw)
        except ValueError:  # partial / corrupt file: ignored, swept by TTL
            return None
        return record if isinstance(record, dict) else None

    def get(
        self, key: str, *, allow_expired: bool = False, local_only: bool = False
    ) -> Optional[dict]:
        """File layer, then durable layer (backfilling the file). None on miss.

        ``allow_expired`` returns an expired FILE record too, for callers that
        answer "expired" differently from "unknown" (the job poll does).
        ``local_only`` skips the durable layer (no network on hot paths).
        """
        if not layers_enabled():
            return None
        try:
            path = self._files.path(key)
            if path is None:
                return None
            now = time.time()
            record = self._decode(self._files.read(path))
            if _created_of(record, self._created_field) is None:
                record = None  # corrupt, partial or foreign: treat as a miss
            if record is not None:
                if allow_expired or self._fresh(record, now):
                    return record
                return None  # expired: the durable copy has the same creation time
            if self._durable is None or local_only:
                return None
            record = self._durable.get(key)
            if record is None or not self._fresh(record, now):
                return None
            encoded = self._encode(key, record)
            if encoded is not None and self._files.write(path, encoded[0], encoded[1]):
                self._files.sweep()  # backfills count against the caps too
            return record
        except Exception as exc:  # error isolation: a store bug must never 500 a request
            logger.error(f"shared_state[{self.name}].get failed: {exc.__class__.__name__}", exc_info=True)
            return None

    def put(self, key: str, record: dict, *, durable: bool = True) -> bool:
        """Write ``record`` to the file layer (synchronously) and queue it for
        the durable layer. Returns True if the file layer accepted it."""
        if not layers_enabled():
            return False
        try:
            path = self._files.path(key)
            if path is None:
                return False
            encoded = self._encode(key, record)
            if encoded is None:
                return False
            data, created = encoded
            is_new = self._files.mtime(path) is None
            written = self._files.write(path, data, created)
            if written and is_new:
                self._files.sweep()
            if durable and self._durable is not None:
                self._durable.put_async(key, data, created)
            return written
        except Exception as exc:  # error isolation
            logger.error(f"shared_state[{self.name}].put failed: {exc.__class__.__name__}", exc_info=True)
            return False

    def delete(self, key: str) -> None:
        """Remove the file-layer copy (durable rows expire on their own)."""
        path = self._files.path(key)
        if path is not None:
            self._files.unlink(path)

    def sweep(self) -> int:
        if not layers_enabled():
            return 0
        try:
            return self._files.sweep()
        except Exception as exc:  # error isolation
            logger.error(f"shared_state[{self.name}].sweep failed: {exc.__class__.__name__}", exc_info=True)
            return 0


class SharedBlobStore:
    """File-only store for large byte payloads (generated plan ZIPs).

    Deliberately never durable: tens of MB of base64 in a JSONB cache row is
    the wrong store (the S47 ``nova_generated_plans`` table already keeps the
    ZIPs across restarts). Bounded per blob, by count, and by total bytes.
    """

    def __init__(
        self,
        name: str,
        base_dir: Callable[[], str],
        *,
        subdir: str,
        key_pattern: "re.Pattern[str]",
        ttl_seconds: float,
        max_blob_bytes: int,
        max_blobs: int,
        max_total_bytes: int,
    ) -> None:
        self.name = name
        self._files = _FileDir(
            name,
            base_dir,
            subdir=subdir,
            prefix="",
            suffix=_BLOB_SUFFIX,
            key_pattern=key_pattern,
            ttl_seconds=ttl_seconds,
            max_entry_bytes=max_blob_bytes,
            max_entries=max_blobs,
            max_total_bytes=max_total_bytes,
        )

    def exists(self, key: str) -> bool:
        path = self._files.path(key)
        return path is not None and self._files.mtime(path) is not None

    def put(self, key: str, data: bytes, created: float) -> bool:
        if not layers_enabled() or not isinstance(data, (bytes, bytearray)) or not data:
            return False
        try:
            path = self._files.path(key)
            if path is None:
                return False
            if len(data) > self._files.max_entry_bytes:
                _log_once(
                    f"blob-too-big-{self.name}",
                    logging.INFO,
                    f"shared_state[{self.name}]: blob over the {self._files.max_entry_bytes}-byte cap; not shared",
                )
                return False
            if not self._files.write(path, bytes(data), float(created)):
                return False
            self._files.sweep()
            return True
        except Exception as exc:  # error isolation
            logger.error(f"shared_state[{self.name}].put failed: {exc.__class__.__name__}", exc_info=True)
            return False

    def get(self, key: str) -> Optional[bytes]:
        if not layers_enabled():
            return None
        try:
            path = self._files.path(key)
            if path is None:
                return None
            created = self._files.mtime(path)
            if created is None or time.time() - created > self._files.ttl_seconds:
                return None
            return self._files.read(path) or None
        except Exception as exc:  # error isolation
            logger.error(f"shared_state[{self.name}].get failed: {exc.__class__.__name__}", exc_info=True)
            return None

    def sweep(self) -> int:
        if not layers_enabled():
            return 0
        try:
            return self._files.sweep()
        except Exception as exc:  # error isolation
            logger.error(f"shared_state[{self.name}].sweep failed: {exc.__class__.__name__}", exc_info=True)
            return 0
