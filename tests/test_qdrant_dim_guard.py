"""Tests for the Qdrant embedding-dimension guard in vector_search.

Scenario: the ACTIVE collection already exists in Qdrant but was created at a
different vector size / distance than the active embedding model produces (a
dim constant changed without a rename, a hand-made collection, VOYAGE_EMBED_DIM
set for an unknown model).  Before the guard this was silent: the worker
attached, upserts were rejected by Qdrant (or scored on the wrong metric), and
/api/deploy/ready looked healthy.

The boundary here is a REAL local HTTP server speaking the slice of Qdrant's
REST API this module uses (GET/PUT collection, PUT points, POST search/count),
so ``_qdrant_request`` runs unmodified.  Loopback is allowed by
tests/conftest.py's network guard; nothing external is contacted.

Contract under test:
  * dims/distance match        -> attaches, ``dim_guard`` is None
  * mismatch (size/distance/named vectors) -> NOT attached; ONE error log;
    ``last_qdrant_error`` + ``dim_guard`` surfaced on /api/deploy/ready;
    zero upserts and zero searches reach the collection; search() falls back to
    BM25; nothing raises
  * Qdrant down / unreadable config -> pre-guard behaviour, unchanged
"""

from __future__ import annotations

import json
import logging
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import vector_search as vs  # noqa: E402

ACTIVE_COLLECTION = "nova_knowledge__voyage-4-lite_1024"


# ---------------------------------------------------------------------------
# Fake Qdrant (real HTTP, loopback)
# ---------------------------------------------------------------------------


class FakeQdrant:
    """Minimal Qdrant REST double; records every request it receives."""

    def __init__(
        self,
        vectors: Any = None,
        exists: bool = True,
        omit_config: bool = False,
    ) -> None:
        self.vectors: Any = (
            {"size": 1024, "distance": "Cosine"} if vectors is None else vectors
        )
        self.exists = exists
        self.omit_config = omit_config
        self.requests: List[Tuple[str, str]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:  # silence
                return

            def _reply(self, code: int, payload: Dict[str, Any]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _handle(self, method: str) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                outer.requests.append((method, self.path))
                parts = [p for p in self.path.split("/") if p]
                if parts[:1] != ["collections"] or len(parts) < 2:
                    return self._reply(404, {"status": {"error": "not found"}})
                if len(parts) == 2:  # /collections/{name}
                    if method == "GET":
                        if not outer.exists:
                            return self._reply(404, {"status": {"error": "Not found"}})
                        info: Dict[str, Any] = {"status": "green", "points_count": 5}
                        if not outer.omit_config:
                            info["config"] = {"params": {"vectors": outer.vectors}}
                        return self._reply(200, {"result": info, "status": "ok"})
                    if method == "PUT":
                        outer.exists = True
                        return self._reply(200, {"result": True, "status": "ok"})
                tail = "/".join(parts[2:])
                if tail == "points" and method == "PUT":
                    return self._reply(200, {"result": {"status": "completed"}})
                if tail == "points/search" and method == "POST":
                    hit = {
                        "id": 1,
                        "score": 0.9,
                        "payload": {"doc_id": "kb.json:1", "text": "vector hit"},
                    }
                    return self._reply(200, {"result": [hit]})
                if tail == "points/count" and method == "POST":
                    return self._reply(200, {"result": {"count": 5}})
                return self._reply(404, {"status": {"error": "not found"}})

            def do_GET(self) -> None:
                self._handle("GET")

            def do_PUT(self) -> None:
                self._handle("PUT")

            def do_POST(self) -> None:
                self._handle("POST")

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def count(self, method: str, suffix: str) -> int:
        return sum(1 for m, p in self.requests if m == method and p.endswith(suffix))

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture()
def qdrant_env(monkeypatch):
    """Isolate every module global the attach/guard path touches."""
    monkeypatch.delenv("EMBEDDING_PROVIDER", raising=False)
    monkeypatch.setattr(vs, "_VOYAGE_MODEL", "voyage-4-lite")
    monkeypatch.setattr(vs, "_QDRANT_API_KEY", "test-key")
    monkeypatch.setattr(vs, "_qdrant_available", False)
    monkeypatch.setattr(vs, "_qdrant_attach_last_attempt", 0.0)
    monkeypatch.setattr(vs, "_qdrant_dim_guard", None)
    monkeypatch.setattr(vs, "_qdrant_dim_guard_logged", set())
    monkeypatch.setattr(vs, "_last_qdrant_error", None)
    monkeypatch.setattr(vs, "_index", [])
    monkeypatch.setattr(vs, "_bm25_index", vs.BM25Index())
    monkeypatch.setattr(vs, "_tfidf_built", False)
    servers: List[FakeQdrant] = []

    def start(**kwargs: Any) -> FakeQdrant:
        fake = FakeQdrant(**kwargs)
        servers.append(fake)
        monkeypatch.setattr(vs, "_QDRANT_URL", fake.url)
        return fake

    yield start
    for fake in servers:
        try:
            fake.stop()
        except OSError:
            pass


def _reset_cooldown(monkeypatch) -> None:
    monkeypatch.setattr(vs, "_qdrant_attach_last_attempt", 0.0)


def _guard_logs(caplog) -> List[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "DIM GUARD" in r.getMessage()
    ]


def _deploy_ready() -> Tuple[Dict[str, Any], int]:
    import routes.health as rh

    captured: Dict[str, Any] = {}

    def _send(handler, result, status_code=200):
        captured["payload"] = result
        captured["status"] = status_code

    with mock.patch.object(rh, "_send_json_response", _send):
        rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)
    return captured["payload"], captured["status"]


# ---------------------------------------------------------------------------
# Compatible collection -> attaches
# ---------------------------------------------------------------------------


def test_matching_collection_attaches_and_reports_no_guard(qdrant_env):
    fake = qdrant_env()  # size 1024 / Cosine == voyage-4-lite
    assert vs._qdrant_attach() is True
    assert vs._qdrant_available is True
    assert vs._qdrant_dim_guard is None
    assert fake.count("GET", f"/collections/{ACTIVE_COLLECTION}") == 1
    assert fake.count("PUT", "") == 0  # attach is read-only

    payload, _ = _deploy_ready()
    assert payload["embedding"]["dim_guard"] is None
    assert payload["retrieval"]["qdrant_attached"] is True


def test_matching_collection_serves_vector_hits(qdrant_env):
    fake = qdrant_env()
    with mock.patch.object(vs, "embed_text", return_value=[0.1] * 1024):
        results = vs.search("anything", top_k=3)
    assert results and results[0]["id"] == "kb.json:1"
    assert fake.count("POST", "/points/search") == 1


def test_ensure_collection_on_matching_collection_is_a_noop(qdrant_env):
    fake = qdrant_env()
    assert vs._qdrant_ensure_collection() is True
    assert vs._qdrant_available is True
    assert fake.count("PUT", ACTIVE_COLLECTION) == 0  # no re-create


# ---------------------------------------------------------------------------
# Mismatch -> guarded, surfaced, never used
# ---------------------------------------------------------------------------


def test_size_mismatch_is_guarded_and_surfaced(qdrant_env, caplog):
    fake = qdrant_env(vectors={"size": 512, "distance": "Cosine"})
    with caplog.at_level(logging.ERROR, logger="vector_search"):
        assert vs._qdrant_attach() is False
    assert vs._qdrant_available is False
    guard = vs._qdrant_dim_guard
    assert guard["collection"] == ACTIVE_COLLECTION
    assert guard["expected_dim"] == 1024
    assert guard["actual_dim"] == 512
    assert guard["model"] == "voyage-4-lite"
    assert "512" in guard["reason"] and "1024" in guard["reason"]
    assert vs._last_qdrant_error.startswith("dim guard:")
    assert len(_guard_logs(caplog)) == 1
    assert fake.count("PUT", "") == 0 and fake.count("POST", "") == 0

    payload, status = _deploy_ready()
    emb = payload["embedding"]
    assert emb["dim_guard"]["actual_dim"] == 512
    assert emb["last_qdrant_error"].startswith("dim guard:")
    assert payload["retrieval"]["qdrant_attached"] is False
    # Readiness is observability-only: a guarded collection must not 503 a deploy.
    checks = payload["checks"]
    assert "dim_guard" not in checks
    assert payload["ready"] == (
        checks["warmup_complete"] and checks["knowledge_base_loaded"]
    )
    assert status == (200 if payload["ready"] else 503)
    assert vs.get_status()["qdrant_dim_guard"]["actual_dim"] == 512


def test_mismatch_logs_exactly_one_error_across_retries(
    qdrant_env, caplog, monkeypatch
):
    qdrant_env(vectors={"size": 768, "distance": "Cosine"})
    with caplog.at_level(logging.ERROR, logger="vector_search"):
        for _ in range(4):  # each cooldown window re-runs the GET + check
            _reset_cooldown(monkeypatch)
            assert vs._qdrant_attach() is False
    assert len(_guard_logs(caplog)) == 1


def test_guard_refuses_upsert_and_search_even_if_flag_is_stale(qdrant_env):
    fake = qdrant_env(vectors={"size": 512, "distance": "Cosine"})
    vs._qdrant_attach()
    # Simulate a stale/forked availability flag: the guard must still hold.
    with mock.patch.object(vs, "_qdrant_available", True):
        assert vs._qdrant_upsert_points([{"id": 1, "vector": [0.0] * 1024}]) is False
        assert vs._qdrant_search([0.0] * 1024, top_k=3) is None
    assert fake.count("PUT", "/points") == 0
    assert fake.count("POST", "/points/search") == 0


def test_ensure_collection_refuses_mismatch_and_build_index_never_upserts(qdrant_env):
    fake = qdrant_env(vectors={"size": 512, "distance": "Cosine"})
    assert vs._qdrant_ensure_collection() is False
    assert vs._qdrant_available is False
    docs = [{"id": "d1", "text": "welding apprenticeship pipelines for fabrication"}]
    with mock.patch.object(vs, "embed_batch", return_value=[[0.1] * 1024]):
        vs.build_index(docs)
    assert (
        fake.count("PUT", "/points") == 0
    ), "must never upsert into a wrong-dim collection"
    assert fake.count("PUT", ACTIVE_COLLECTION) == 0, "must not re-create/overwrite it"


def test_search_falls_back_to_bm25_without_embedding_or_qdrant_calls(qdrant_env):
    fake = qdrant_env(vectors={"size": 512, "distance": "Cosine"})
    vs._bm25_index.index(
        [
            {"id": "d1", "text": "welding apprenticeship pipelines", "metadata": {}},
            {"id": "d2", "text": "nursing recruitment advertising", "metadata": {}},
        ]
    )
    with mock.patch.object(vs, "embed_text") as embed:
        results = vs.search("nursing recruitment", top_k=2)
    assert results and results[0]["id"] == "d2"
    assert results[0]["search_method"] == "bm25"
    embed.assert_not_called()  # no Voyage spend on a collection we refuse to query
    assert fake.count("POST", "/points/search") == 0


def test_distance_mismatch_is_guarded(qdrant_env):
    qdrant_env(vectors={"size": 1024, "distance": "Dot"})
    assert vs._qdrant_attach() is False
    assert vs._qdrant_dim_guard["actual_distance"] == "Dot"
    assert "Dot" in vs._qdrant_dim_guard["reason"]


def test_distance_check_is_case_insensitive(qdrant_env):
    qdrant_env(vectors={"size": 1024, "distance": "cosine"})
    assert vs._qdrant_attach() is True
    assert vs._qdrant_dim_guard is None


def test_named_vector_collection_is_guarded(qdrant_env):
    qdrant_env(vectors={"text": {"size": 1024, "distance": "Cosine"}})
    assert vs._qdrant_attach() is False
    assert vs._qdrant_dim_guard["named_vectors"] is True


def test_guard_is_provider_aware_gemini_expects_768(qdrant_env, monkeypatch):
    monkeypatch.setenv("EMBEDDING_PROVIDER", "gemini")
    qdrant_env(vectors={"size": 1024, "distance": "Cosine"})
    assert vs._qdrant_attach() is False
    assert vs._qdrant_dim_guard["expected_dim"] == 768
    assert vs._qdrant_dim_guard["collection"] == vs._active_collection()


def test_guard_clears_and_attaches_once_the_collection_is_fixed(
    qdrant_env, monkeypatch
):
    fake = qdrant_env(vectors={"size": 512, "distance": "Cosine"})
    assert vs._qdrant_attach() is False
    assert vs._last_qdrant_error.startswith("dim guard:")
    fake.vectors = {"size": 1024, "distance": "Cosine"}  # operator recreates it
    _reset_cooldown(monkeypatch)
    assert vs._qdrant_attach() is True
    assert vs._qdrant_dim_guard is None
    assert vs._last_qdrant_error is None  # the stale guard error is cleared


# ---------------------------------------------------------------------------
# Unchanged behaviour: Qdrant down / unreadable config
# ---------------------------------------------------------------------------


def test_qdrant_down_behaves_as_before_and_sets_no_guard(qdrant_env, monkeypatch):
    fake = qdrant_env()
    fake.stop()  # connection refused on loopback
    assert vs._qdrant_attach() is False
    assert vs._qdrant_available is False
    assert vs._qdrant_dim_guard is None
    assert vs._last_qdrant_error is None  # attach never recorded an error before either
    payload, _ = _deploy_ready()  # observability must not raise either
    assert payload["embedding"]["dim_guard"] is None


def test_collection_missing_is_not_a_dim_guard(qdrant_env):
    qdrant_env(exists=False)
    assert vs._qdrant_attach() is False  # 404 -> same as before
    assert vs._qdrant_dim_guard is None


def test_unreadable_config_fails_open_like_before(qdrant_env):
    qdrant_env(omit_config=True)  # e.g. an older Qdrant without params
    assert vs._qdrant_attach() is True
    assert vs._qdrant_dim_guard is None


@pytest.mark.parametrize(
    "weird",
    ["a string", 1024, ["list"], {"size": "abc"}, {"size": None, "distance": 5}],
)
def test_weird_config_shapes_never_raise(qdrant_env, weird):
    qdrant_env(vectors=weird)
    result = vs._qdrant_attach()  # must not raise, whatever Qdrant returns
    assert result in (True, False)
    assert vs._qdrant_dim_guard is None  # unverifiable -> fail open, not a guard


def test_guard_helper_is_pure_over_its_input():
    """No network: the helper only reads the dict it is given."""
    ok = {"config": {"params": {"vectors": {"size": 1024, "distance": "Cosine"}}}}
    bad = {"config": {"params": {"vectors": {"size": 8, "distance": "Cosine"}}}}
    with mock.patch.object(vs, "_qdrant_dim_guard", None), mock.patch.object(
        vs, "_qdrant_dim_guard_logged", set()
    ), mock.patch.object(vs, "_last_qdrant_error", None):
        assert vs._qdrant_dim_guard_check(ACTIVE_COLLECTION, ok) is False
        assert vs._qdrant_dim_guard_check(ACTIVE_COLLECTION, bad) is True
        assert vs._qdrant_dim_guard["actual_dim"] == 8
        assert vs._qdrant_dim_guard_check(ACTIVE_COLLECTION, None) is False
        assert vs._qdrant_dim_guard is None
