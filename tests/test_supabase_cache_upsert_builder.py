"""Regression tests: supabase_cache writes must upsert on ``key``.

Prod telemetry 2026-10-01 (ledger A_prod_telemetry section 2 row 6): 54
``supabase_cache _record_error: HTTP 409 duplicate key ... cache_key_key`` per
week.

Root cause: ``cache_set`` / ``cache_set_many`` POST to ``/rest/v1/cache`` with
``Prefer: resolution=merge-duplicates`` but WITHOUT ``?on_conflict=key``.
PostgREST then resolves the conflict against the table's PRIMARY KEY, which is
``id`` (``cache.key`` is a separate ``UNIQUE`` -- the ``cache_key_key``
constraint). The incoming row carries no ``id``, so there is no PK conflict, the
INSERT runs, and Postgres raises a unique violation on ``cache_key_key`` -> 409.
Any second write of a key (an expired entry being refreshed, two workers
writing the same key) therefore failed, leaving the stale/expired row in place
until the 6-hourly cleanup deleted it -- i.e. the L3 cache silently could not be
refreshed, which also re-triggers upstream API fetches.

The fake PostgREST below reproduces exactly that resolution rule.
"""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.parse
from email.message import Message
from typing import Any

import pytest

import supabase_cache as sc

BASE = "https://example.supabase.co"


class _Resp:
    def __init__(self, status: int = 201, body: bytes = b"") -> None:
        self.status = status
        self._body = body

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def read(self, *a: Any) -> bytes:
        return self._body


class _FakePostgrestCache:
    """``cache`` table: PRIMARY KEY (id), UNIQUE (key) -- as in prod."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.requests: list[Any] = []

    def __call__(self, req: Any, timeout: Any = None, context: Any = None) -> Any:
        self.requests.append(req)
        parsed = urllib.parse.urlparse(req.full_url)
        assert parsed.path == "/rest/v1/cache", parsed.path
        if req.get_method() != "POST":
            return _Resp(200, b"[]")
        qs = urllib.parse.parse_qs(parsed.query)
        on_conflict = (qs.get("on_conflict") or [""])[0]
        merge = "merge-duplicates" in (req.get_header("Prefer") or "")
        rows = json.loads(req.data)
        rows = rows if isinstance(rows, list) else [rows]
        seen_in_batch: set[str] = set()
        for row in rows:
            key = row["key"]
            if key in seen_in_batch and on_conflict == "key" and merge:
                # "ON CONFLICT DO UPDATE command cannot affect row a second time"
                raise _err(
                    400,
                    b'{"code":"21000","message":"ON CONFLICT DO UPDATE command cannot affect row a second time"}',
                )
            seen_in_batch.add(key)
            exists = key in self.rows
            if exists and merge and on_conflict == "key":
                self.rows[key].update(row)  # ON CONFLICT (key) DO UPDATE
            elif exists:
                # conflict target defaults to the PK (id); the row has no id, so
                # the INSERT runs and trips the UNIQUE(key) constraint
                raise _err(
                    409,
                    b'{"code":"23505","message":"duplicate key value violates '
                    b'unique constraint \\"cache_key_key\\""}',
                )
            else:
                self.rows[key] = dict(row)
        return _Resp(201)


def _err(code: int, body: bytes) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        f"{BASE}/rest/v1/cache", code, "err", Message(), io.BytesIO(body)
    )


@pytest.fixture()
def fake(monkeypatch: pytest.MonkeyPatch) -> _FakePostgrestCache:
    f = _FakePostgrestCache()
    monkeypatch.setattr(sc, "_ENABLED", True)
    monkeypatch.setattr(sc, "_SUPABASE_URL", BASE)
    monkeypatch.setattr(sc, "_SUPABASE_ANON_KEY", "test-anon-key")
    monkeypatch.setattr(sc.time, "sleep", lambda s: None)
    monkeypatch.setattr(sc.urllib.request, "urlopen", f)
    return f


# ---------------------------------------------------------------------------
# Request builder
# ---------------------------------------------------------------------------


def test_cache_set_targets_the_key_unique_constraint(fake: _FakePostgrestCache) -> None:
    assert sc.cache_set("plan:abc", {"v": 1}, ttl_seconds=60, category="api") is True
    req = fake.requests[0]
    assert req.full_url == f"{BASE}/rest/v1/cache?on_conflict=key", req.full_url
    assert req.get_method() == "POST"
    prefer = req.get_header("Prefer") or ""
    assert "resolution=merge-duplicates" in prefer and "return=minimal" in prefer
    body = json.loads(req.data)
    assert body["key"] == "plan:abc" and body["data"] == {"v": 1}
    assert body["category"] == "api" and body["hit_count"] == 0


def test_cache_set_many_targets_the_key_unique_constraint(
    fake: _FakePostgrestCache,
) -> None:
    n = sc.cache_set_many([{"key": "a", "data": 1}, {"key": "b", "data": 2}])
    assert n == 2
    req = fake.requests[0]
    assert req.full_url == f"{BASE}/rest/v1/cache?on_conflict=key", req.full_url
    assert "resolution=merge-duplicates" in (req.get_header("Prefer") or "")


# ---------------------------------------------------------------------------
# Behaviour against a PostgREST that resolves conflicts like prod does
# ---------------------------------------------------------------------------


def test_rewriting_an_existing_key_succeeds_and_replaces_the_value(
    fake: _FakePostgrestCache,
) -> None:
    assert sc.cache_set("bls:15-1252", {"median": 100}) is True
    assert sc.cache_set("bls:15-1252", {"median": 200}) is True  # was 409
    assert fake.rows["bls:15-1252"]["data"] == {"median": 200}
    assert len(fake.rows) == 1


def test_two_workers_writing_the_same_key_never_error(
    fake: _FakePostgrestCache,
) -> None:
    results = [sc.cache_set("same", {"w": i}) for i in range(5)]
    assert all(results)
    assert len(fake.rows) == 1


def test_batch_rewrite_of_existing_keys_succeeds(fake: _FakePostgrestCache) -> None:
    assert sc.cache_set_many([{"key": "a", "data": 1}]) == 1
    assert sc.cache_set_many([{"key": "a", "data": 2}, {"key": "b", "data": 3}]) == 2
    assert fake.rows["a"]["data"] == 2 and fake.rows["b"]["data"] == 3


def test_batch_with_duplicate_keys_is_deduped_last_wins(
    fake: _FakePostgrestCache,
) -> None:
    n = sc.cache_set_many(
        [
            {"key": "dup", "data": "first"},
            {"key": "dup", "data": "last"},
            {"key": "x", "data": 1},
        ]
    )
    assert (
        n == 2
    ), "rows actually stored (Postgres rejects a batch that hits one key twice)"
    assert fake.rows["dup"]["data"] == "last"
    sent = json.loads(fake.requests[0].data)
    assert [r["key"] for r in sent] == ["dup", "x"]


def test_reads_are_unaffected(fake: _FakePostgrestCache) -> None:
    assert sc.cache_get("nothing") is None  # GET path untouched by the change
