"""Tests for the Gemini embedding provider path in vector_search (Layer-3 #11).

Covers the EMBEDDING_PROVIDER switch, provider-aware vector dimension
selection, provider-namespaced cache keys, model-scoped Qdrant collection
naming (_active_collection), the Gemini batchEmbedContents
request/parse/retry behavior (including the outputDimensionality contract
and response-shape validation), and graceful failure -- all with the network
mocked (no live API calls). Verifies Voyage stays the default and behaves
exactly as before when EMBEDDING_PROVIDER != "gemini".

Model lifecycle: the active Gemini model is gemini-embedding-2.
text-embedding-004 was shut down 2026-01-14 and gemini-embedding-001's
documented shutdown date was 2026-07-14 -- both predecessor models are dead,
which is why the active model and its assertions below target
gemini-embedding-2, not either retired name.

Runs under pytest, or standalone: ``python3 tests/test_gemini_embeddings.py``.
"""

import io
import json
import sys
import urllib.error
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import vector_search as vs  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────────────


class _FakeResp:
    """Minimal context-manager stand-in for urllib.request.urlopen()."""

    def __init__(self, payload: dict):
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self._body


def _gemini_embeddings_payload(vectors):
    """Shape a fake batchEmbedContents response."""
    return {"embeddings": [{"values": v} for v in vectors]}


def _env(provider):
    """Patch EMBEDDING_PROVIDER (and clear it cleanly afterwards)."""
    return mock.patch.dict("os.environ", {"EMBEDDING_PROVIDER": provider}, clear=False)


def _no_cache():
    """Force every embed_batch lookup to miss and skip all disk I/O."""
    return mock.patch.multiple(
        vs,
        _load_embedding_cache=mock.Mock(),
        _cache_get_locked=mock.Mock(return_value=None),
        _cache_put_locked=mock.Mock(),
        _ensure_flush_thread=mock.Mock(),
    )


# ── Provider switch ──────────────────────────────────────────────────────────


def test_default_provider_is_voyage_pending_key_unblock():
    """The DEFAULT lives in code (render.yaml envVars are not applied by
    Render for this service -- proven live 2026-07-21). The gemini cutover
    reached prod and was rolled back the same day after the deploy/ready
    embedding block captured its blocker: the prod GEMINI_API_KEY is
    Google-console restricted and 403-blocks BatchEmbedContents. Until the
    key allows embeddings, the default stays voyage (populated index beats
    an empty one); flip this test together with the default when re-cutting.
    """
    with mock.patch.dict("os.environ", {}, clear=False):
        import os

        os.environ.pop("EMBEDDING_PROVIDER", None)
        assert vs.get_embedding_provider() == "voyage"
        assert vs.get_active_embedding_model() == vs._VOYAGE_MODEL


def test_gemini_selectable_via_env_override():
    with _env("gemini"):
        assert vs.get_embedding_provider() == "gemini"
        assert vs.get_active_embedding_model() == vs._GEMINI_EMBED_MODEL


def test_gemini_provider_selected():
    with _env("gemini"):
        assert vs.get_embedding_provider() == "gemini"
        assert vs.get_active_embedding_model() == "gemini-embedding-2"
        assert vs._active_vector_dim() == 768


def test_provider_value_is_case_insensitive_and_safe():
    with _env("GEMINI"):
        assert vs.get_embedding_provider() == "gemini"
    with _env("VOYAGE"):
        assert vs.get_embedding_provider() == "voyage"
    # Unknown values resolve to the DEFAULT provider (voyage while the prod
    # GEMINI_API_KEY blocks embeddings) -- never crash, never half-recognized.
    # Dim is read dynamically (not hardcoded) because this test is about
    # provider-string resolution, not about which Voyage model is active.
    with _env("openai"):
        assert vs.get_embedding_provider() == "voyage"
        assert vs._active_vector_dim() == vs._voyage_embed_dim()


# ── Dimension selection drives Qdrant collection creation ────────────────────


def test_collection_created_at_gemini_dim():
    captured = {}

    def fake_request(method, path, body=None, timeout=vs._QDRANT_TIMEOUT):
        if method == "GET":
            return {"result": None}  # collection does not exist yet
        captured["method"] = method
        captured["body"] = body
        return {"result": True}

    with _env("gemini"), mock.patch.multiple(
        vs,
        _qdrant_is_configured=mock.Mock(return_value=True),
        _qdrant_request=mock.Mock(side_effect=fake_request),
    ):
        assert vs._qdrant_ensure_collection() is True

    assert captured["method"] == "PUT"
    assert captured["body"]["vectors"]["size"] == 768


def test_collection_created_at_voyage_dim():
    """Collection creation must use whichever dim the active Voyage model
    resolves to (1024 for the voyage-4-lite default) -- read dynamically so
    this test doesn't hardcode a model-specific width."""
    captured = {}

    def fake_request(method, path, body=None, timeout=vs._QDRANT_TIMEOUT):
        if method == "GET":
            return {"result": None}
        captured["body"] = body
        return {"result": True}

    with _env("voyage"), mock.patch.multiple(
        vs,
        _qdrant_is_configured=mock.Mock(return_value=True),
        _qdrant_request=mock.Mock(side_effect=fake_request),
    ):
        assert vs._qdrant_ensure_collection() is True

    assert captured["body"]["vectors"]["size"] == vs._voyage_embed_dim()


def test_collection_created_at_voyage_3_lite_legacy_dim():
    """The legacy rollback target (voyage-3-lite) must still create its
    collection at the fixed 512-dim width it has always used."""
    captured = {}

    def fake_request(method, path, body=None, timeout=vs._QDRANT_TIMEOUT):
        if method == "GET":
            return {"result": None}
        captured["body"] = body
        return {"result": True}

    with _env("voyage"), mock.patch.object(
        vs, "_VOYAGE_MODEL", "voyage-3-lite"
    ), mock.patch.multiple(
        vs,
        _qdrant_is_configured=mock.Mock(return_value=True),
        _qdrant_request=mock.Mock(side_effect=fake_request),
    ):
        assert vs._qdrant_ensure_collection() is True

    assert captured["body"]["vectors"]["size"] == 512


# ── Qdrant collection is scoped per embedding space ──────────────────────────


def test_active_collection_voyage_is_legacy_name():
    # Only voyage-3-lite (the pre-succession model) keeps the bare legacy
    # name -- it's the collection production served from before the
    # voyage-4-lite default, kept as the instant-rollback target. Pin the
    # model explicitly: the "voyage" provider alone no longer implies the
    # legacy name now that voyage-4-lite is the default model.
    with _env("voyage"), mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-3-lite"):
        assert vs._active_collection() == "nova_knowledge"


def test_active_collection_gemini_is_model_scoped():
    with _env("gemini"):
        assert vs._active_collection() == "nova_knowledge__gemini-embedding-2_768"

        # A future model succession (GEMINI_EMBED_MODEL=<new>) must
        # auto-scope a brand new collection name, so it can never collide
        # with -- or overwrite -- the previous model's serving collection.
        with mock.patch.object(vs, "_GEMINI_EMBED_MODEL", "gemini-embedding-3"):
            assert "gemini-embedding-3" in vs._active_collection()


# ── Cache keys are namespaced by provider/model ──────────────────────────────


def test_cache_key_differs_by_provider():
    with _env("voyage"):
        voyage_key = vs._text_cache_key("driver jobs in Dallas")
    with _env("gemini"):
        gemini_key = vs._text_cache_key("driver jobs in Dallas")
    assert voyage_key != gemini_key  # different spaces must not collide


# ── Gemini batchEmbedContents request/parse ──────────────────────────────────


def test_embed_batch_gemini_parses_vectors():
    fake_vectors = [[0.1] * 768, [0.2] * 768]
    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "test-key"}, clear=False
    ), mock.patch.object(
        vs.urllib.request,
        "urlopen",
        return_value=_FakeResp(_gemini_embeddings_payload(fake_vectors)),
    ) as m:
        out = vs._embed_batch_gemini(["a", "b"])

    assert out == fake_vectors
    # Key must be passed via the ?key= query param (Google convention),
    # and the model must be gemini-embedding-2.
    sent_req = m.call_args.args[0]
    assert "batchEmbedContents?key=test-key" in sent_req.full_url
    assert "gemini-embedding-2" in sent_req.full_url


def test_gemini_payload_requests_output_dimensionality():
    """Every request entry must ask for 768-dim output at the right model.

    outputDimensionality is what keeps gemini-embedding-2's native 3072-dim
    vectors down to the 768-dim the Qdrant collection is created at --
    dropping this field would silently poison the index with wide vectors.
    """
    captured = {}

    def _fake_urlopen(req, timeout=None, context=None):
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return _FakeResp(_gemini_embeddings_payload([[0.1] * 768, [0.2] * 768]))

    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "test-key"}, clear=False
    ), mock.patch.object(vs.urllib.request, "urlopen", side_effect=_fake_urlopen):
        out = vs._embed_batch_gemini(["a", "b"])

    assert out == [[0.1] * 768, [0.2] * 768]
    requests_sent = captured["body"]["requests"]
    assert len(requests_sent) == 2
    for entry in requests_sent:
        # Both documented spellings, identical values: flat (deprecated but
        # in-schema) and embedContentConfig (current per ai.google.dev/api/
        # embeddings) -- whichever the server honors yields 768.
        assert entry["outputDimensionality"] == 768
        assert entry["embedContentConfig"] == {"outputDimensionality": 768}
        assert entry["model"] == "models/gemini-embedding-2"


def test_gemini_wrong_dim_response_rejected():
    """A response at the model's native 3072 dim (outputDimensionality
    ignored/renamed upstream) must be refused, not cached or upserted --
    mixing dims in one collection corrupts search.
    """
    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "k"}, clear=False
    ), mock.patch.object(
        vs.urllib.request,
        "urlopen",
        return_value=_FakeResp(_gemini_embeddings_payload([[0.1] * 3072])),
    ):
        assert vs._embed_batch_gemini(["a"]) is None

    # Through embed_batch, the rejection must also mean nothing gets written
    # into the disk cache (the wrong-dim vector never reaches that code path).
    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "k"}, clear=False
    ), _no_cache(), mock.patch.object(vs, "_embed_batch_gemini", return_value=None):
        assert vs.embed_batch(["hello"]) is None
        vs._cache_put_locked.assert_not_called()


def test_gemini_count_mismatch_rejected():
    # Asked for 3 texts, Gemini returns only 2 embeddings back (each at the
    # correct 768 dim) -- a count mismatch is its own contract violation,
    # independent of the dim check, and must also fail closed to None.
    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "k"}, clear=False
    ), mock.patch.object(
        vs.urllib.request,
        "urlopen",
        return_value=_FakeResp(_gemini_embeddings_payload([[0.1] * 768, [0.2] * 768])),
    ):
        assert vs._embed_batch_gemini(["a", "b", "c"]) is None


def test_embed_batch_gemini_no_key_returns_none():
    with _env("gemini"), mock.patch.dict("os.environ", {}, clear=False):
        import os

        os.environ.pop("GEMINI_API_KEY", None)
        assert vs._embed_batch_gemini(["a"]) is None


def test_embed_batch_gemini_http_error_returns_none():
    err = urllib.error.HTTPError(
        url="x", code=500, msg="boom", hdrs=None, fp=io.BytesIO(b"")
    )
    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "k"}, clear=False
    ), mock.patch.object(vs.urllib.request, "urlopen", side_effect=err):
        assert vs._embed_batch_gemini(["a"]) is None


def test_embed_batch_gemini_short_response_returns_none():
    # Asked for 2, got 1 back -> graceful None (caller falls back to BM25).
    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "k"}, clear=False
    ), mock.patch.object(
        vs.urllib.request,
        "urlopen",
        return_value=_FakeResp(_gemini_embeddings_payload([[0.1] * 768])),
    ):
        assert vs._embed_batch_gemini(["a", "b"]) is None


# ── embed_batch dispatch routes to the right provider backend ─────────────────


def test_embed_batch_routes_to_gemini():
    gem_vecs = [[0.3] * 768]
    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "k"}, clear=False
    ), _no_cache(), mock.patch.object(
        vs, "_embed_batch_gemini", return_value=gem_vecs
    ) as gem, mock.patch.object(
        vs, "_embed_uncached_voyage"
    ) as voy:
        out = vs.embed_batch(["hello"])

    assert out == gem_vecs
    gem.assert_called_once()
    voy.assert_not_called()  # Voyage path must NOT run under gemini


def test_embed_batch_routes_to_voyage_by_default():
    voy_vecs = [[0.4] * 512]
    # Patch the _VOYAGE_API_KEY override seam directly; mock.patch.object
    # restores it on exit so nothing leaks into later tests. (Env-var
    # patching is also safe now that _get_api_key() reads the environment
    # fresh instead of caching it, but the attr patch is the suite-wide
    # convention for forcing a Voyage key.)
    with _env("voyage"), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ), _no_cache(), mock.patch.object(
        vs, "_embed_uncached_voyage", return_value=voy_vecs
    ) as voy, mock.patch.object(
        vs, "_embed_batch_gemini"
    ) as gem:
        out = vs.embed_batch(["hello"])

    assert out == voy_vecs
    voy.assert_called_once()
    gem.assert_not_called()  # Gemini path must NOT run under voyage


def test_embed_batch_gemini_failure_is_graceful():
    # Provider compute returns None -> embed_batch returns None (no raise).
    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "k"}, clear=False
    ), _no_cache(), mock.patch.object(vs, "_embed_batch_gemini", return_value=None):
        assert vs.embed_batch(["hello"]) is None


def test_embed_batch_empty_input_returns_empty():
    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "k"}, clear=False
    ):
        assert vs.embed_batch([]) == []


# ── Status surfaces the active provider ──────────────────────────────────────


def test_status_reports_provider_and_dim():
    with _env("gemini"):
        st = vs.get_status()
    assert st["embedding_provider"] == "gemini"
    assert st["embedding_model"] == "gemini-embedding-2"
    assert st["embedding_dim"] == 768


if __name__ == "__main__":
    _failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                _failures += 1
                print(f"FAIL {_name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                _failures += 1
                print(f"ERROR {_name}: {exc}")
    sys.exit(1 if _failures else 0)


# ── /api/deploy/ready embedding observability ────────────────────────────────


class _CapturingHandler:
    """Minimal handler stand-in capturing _send_json_response payloads."""

    def __init__(self):
        self.payload = None
        self.status = None


def test_deploy_ready_exposes_embedding_block():
    """The public readiness gate must expose the active embedding space.

    The detailed /api/health is admin-gated (Bug #9), so this block is the
    only public instrument for verifying a provider cutover: provider, model,
    dim, model-scoped collection, and the in-memory indexed_documents counter
    that proves startup indexing filled the new space.
    """
    import routes.health as rh

    captured = _CapturingHandler()

    def _fake_send(handler, result, status_code=200):
        captured.payload = result
        captured.status = status_code

    with mock.patch.object(rh, "_send_json_response", _fake_send), mock.patch.object(
        vs, "_index", [{"id": "d1"}, {"id": "d2"}]
    ), _env("gemini"):
        rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)

    emb = (captured.payload or {}).get("embedding")
    assert emb is not None, "readiness payload must carry the embedding block"
    assert emb["provider"] == "gemini"
    assert emb["model"] == "gemini-embedding-2"
    assert emb["dim"] == 768
    assert emb["collection"] == "nova_knowledge__gemini-embedding-2_768"
    assert emb["indexed_documents"] == 2


# ── last_embed_error observability ───────────────────────────────────────────


def test_http_error_records_redacted_reason_and_success_clears_it():
    """A terminal HTTP error must record a key-redacted reason; the next fully
    successful run must clear it. This is the public breadcrumb for 'index
    stuck at 0' -- the 2026-07-21 cutover had to be diagnosed blind without it.
    """
    import urllib.error as _ue

    def _boom(req, timeout=None, context=None):
        raise _ue.HTTPError(
            "https://generativelanguage.googleapis.com/x?key=SECRET123",
            400,
            "Bad Request",
            None,
            io.BytesIO(b'{"error": {"message": "Unknown name outputDimensionality"}}'),
        )

    with _env("gemini"), mock.patch.dict(
        "os.environ", {"GEMINI_API_KEY": "test-key"}, clear=False
    ):
        with mock.patch.object(vs.urllib.request, "urlopen", side_effect=_boom):
            assert vs._embed_batch_gemini(["a"]) is None
        err = vs._last_embed_error
        assert err is not None
        assert "400" in err
        assert "Unknown name outputDimensionality" in err
        assert "SECRET123" not in err  # key redaction is load-bearing

        # A fully successful run clears the sticky reason.
        def _ok(req, timeout=None, context=None):
            return _FakeResp(_gemini_embeddings_payload([[0.1] * 768]))

        with mock.patch.object(vs.urllib.request, "urlopen", side_effect=_ok):
            assert vs._embed_batch_gemini(["b"]) is not None
        assert vs._last_embed_error is None


def test_deploy_ready_surfaces_last_embed_error():
    import routes.health as rh

    captured = _CapturingHandler()

    def _fake_send(handler, result, status_code=200):
        captured.payload = result
        captured.status = status_code

    with mock.patch.object(rh, "_send_json_response", _fake_send), mock.patch.object(
        vs, "_last_embed_error", "gemini HTTP 400: something"
    ), _env("gemini"):
        rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)

    emb = (captured.payload or {}).get("embedding")
    assert emb is not None
    assert emb["last_embed_error"] == "gemini HTTP 400: something"


# ── Qdrant collection point count (fork-immune readiness signal) ────────────
#
# /api/deploy/ready's indexed_documents field is len(vector_search._index) --
# an in-process counter. Under this app's production deploy (gunicorn
# --preload), the deferred-startup indexing thread runs once in the master
# before fork, and forked workers (the ones actually serving requests)
# inherit whatever _index was at fork time and never see it update again.
# _qdrant_collection_point_count() asks Qdrant itself -- an external service,
# so its answer is correct no matter which process/worker asks.


def test_qdrant_point_count_queries_active_collection():
    captured = {}

    def fake_request(method, path, body=None, timeout=vs._QDRANT_TIMEOUT):
        captured["method"] = method
        captured["path"] = path
        captured["body"] = body
        return {"result": {"count": 6227}, "status": "ok", "time": 0.001}

    with _env("voyage"), mock.patch.object(
        vs, "_VOYAGE_MODEL", "voyage-4-lite"
    ), mock.patch.multiple(
        vs,
        _qdrant_is_configured=mock.Mock(return_value=True),
        _qdrant_request=mock.Mock(side_effect=fake_request),
    ):
        assert vs._qdrant_collection_point_count() == 6227

    assert captured["method"] == "POST"
    assert (
        captured["path"]
        == "/collections/nova_knowledge__voyage-4-lite_1024/points/count"
    )
    assert captured["body"] == {"exact": True}


def test_qdrant_point_count_not_configured_returns_none_without_a_request():
    """Deliberately gated on _qdrant_is_configured() (env presence), NOT
    _qdrant_available -- the latter is only set True inside whichever
    process ran _qdrant_ensure_collection() (the master), so a forked
    worker's copy is exactly as frozen-at-fork as _index. Gating on it here
    would just reintroduce the bug this function exists to route around."""
    with mock.patch.multiple(
        vs,
        _qdrant_is_configured=mock.Mock(return_value=False),
        _qdrant_request=mock.Mock(side_effect=AssertionError("must not be called")),
    ):
        assert vs._qdrant_collection_point_count() is None


def test_qdrant_point_count_returns_none_on_request_failure():
    with mock.patch.multiple(
        vs,
        _qdrant_is_configured=mock.Mock(return_value=True),
        _qdrant_request=mock.Mock(return_value=None),
    ):
        assert vs._qdrant_collection_point_count() is None


def test_qdrant_point_count_returns_none_on_malformed_response():
    """A response missing the expected result.count shape must degrade to
    None, never raise -- a readiness endpoint cannot 500 because Qdrant
    changed its response shape or returned something unexpected."""
    for bad_response in ({}, {"result": {}}, {"result": {"count": "not-an-int"}}):
        with mock.patch.multiple(
            vs,
            _qdrant_is_configured=mock.Mock(return_value=True),
            _qdrant_request=mock.Mock(return_value=bad_response),
        ):
            assert vs._qdrant_collection_point_count() is None


def test_deploy_ready_surfaces_qdrant_point_count():
    """The readiness payload must carry qdrant_point_count ALONGSIDE (never
    instead of) indexed_documents -- the new field is the authoritative
    fork-immune signal, the old one stays for backward compat / the
    in-process view it does correctly reflect."""
    import routes.health as rh

    captured = _CapturingHandler()

    def _fake_send(handler, result, status_code=200):
        captured.payload = result
        captured.status = status_code

    with mock.patch.object(rh, "_send_json_response", _fake_send), mock.patch.object(
        vs, "_index", [{"id": "d1"}, {"id": "d2"}]
    ), mock.patch.object(vs, "_qdrant_collection_point_count", return_value=6227), _env(
        "gemini"
    ):
        rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)

    emb = (captured.payload or {}).get("embedding")
    assert emb is not None
    assert emb["qdrant_point_count"] == 6227
    assert emb["indexed_documents"] == 2  # old field still present, unchanged


def test_deploy_ready_qdrant_point_count_none_when_qdrant_unavailable():
    """Qdrant unreachable/unconfigured must not break the readiness probe --
    qdrant_point_count degrades to None while the rest of the payload (and
    the in-process indexed_documents fallback) stays intact."""
    import routes.health as rh

    captured = _CapturingHandler()

    def _fake_send(handler, result, status_code=200):
        captured.payload = result
        captured.status = status_code

    with mock.patch.object(rh, "_send_json_response", _fake_send), mock.patch.object(
        vs, "_index", [{"id": "d1"}]
    ), mock.patch.object(vs, "_qdrant_collection_point_count", return_value=None), _env(
        "gemini"
    ):
        rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)

    emb = (captured.payload or {}).get("embedding")
    assert emb is not None
    assert emb["qdrant_point_count"] is None
    assert emb["indexed_documents"] == 1


# ── last_qdrant_error observability ──────────────────────────────────────────
#
# Mirrors the last_embed_error tests above. Embedding can succeed while every
# Qdrant write silently fails (_qdrant_upsert_points/_qdrant_ensure_collection
# only logged a warning and returned False) -- that gap was indistinguishable
# from "still slowly embedding" on the public readiness probe: both look like
# qdrant_point_count staying None/0 with last_embed_error null. All tests stub
# _qdrant_request; none touch a real Qdrant instance.


def test_qdrant_upsert_records_reason_and_full_success_clears_it():
    with mock.patch.object(vs, "_qdrant_available", True), mock.patch.object(
        vs, "_qdrant_request", return_value=None
    ):
        assert (
            vs._qdrant_upsert_points([{"id": 1, "vector": [0.1], "payload": {}}])
            is False
        )
    err = vs._last_qdrant_error
    assert err is not None
    assert "upsert batch 0-1" in err

    with mock.patch.object(vs, "_qdrant_available", True), mock.patch.object(
        vs, "_qdrant_request", return_value={"status": "ok"}
    ):
        assert (
            vs._qdrant_upsert_points([{"id": 1, "vector": [0.1], "payload": {}}])
            is True
        )
    assert vs._last_qdrant_error is None


def test_qdrant_upsert_partial_batch_failure_keeps_error_recorded():
    """If any batch in a multi-batch upsert fails, the call reports overall
    failure and must NOT clear a previously recorded error -- a later batch's
    success clearing the signal would mask the earlier batch's real failure."""
    points = [{"id": i, "vector": [0.1], "payload": {}} for i in range(150)]
    calls = []

    def fake_request(method, path, body=None, timeout=vs._QDRANT_TIMEOUT):
        calls.append(body)
        return None if len(calls) == 1 else {"status": "ok"}

    with mock.patch.object(vs, "_qdrant_available", True), mock.patch.object(
        vs, "_qdrant_request", side_effect=fake_request
    ):
        assert vs._qdrant_upsert_points(points) is False
    assert vs._last_qdrant_error is not None
    assert "upsert batch 0-100" in vs._last_qdrant_error


def test_qdrant_ensure_collection_records_reason_on_create_failure():
    def fake_request(method, path, body=None, timeout=vs._QDRANT_TIMEOUT):
        if method == "GET":
            return {"result": None}  # collection does not exist yet
        return None  # PUT create fails

    with _env("gemini"), mock.patch.multiple(
        vs,
        _qdrant_is_configured=mock.Mock(return_value=True),
        _qdrant_request=mock.Mock(side_effect=fake_request),
    ):
        assert vs._qdrant_ensure_collection() is False
    err = vs._last_qdrant_error
    assert err is not None
    assert "collection create failed" in err


def test_qdrant_ensure_collection_success_clears_previous_error():
    with mock.patch.object(vs, "_last_qdrant_error", "stale failure from a prior run"):
        with _env("gemini"), mock.patch.multiple(
            vs,
            _qdrant_is_configured=mock.Mock(return_value=True),
            _qdrant_request=mock.Mock(return_value={"result": True}),
        ):
            assert vs._qdrant_ensure_collection() is True
        assert vs._last_qdrant_error is None


def test_deploy_ready_surfaces_last_qdrant_error():
    import routes.health as rh

    captured = _CapturingHandler()

    def _fake_send(handler, result, status_code=200):
        captured.payload = result
        captured.status = status_code

    with mock.patch.object(rh, "_send_json_response", _fake_send), mock.patch.object(
        vs,
        "_last_qdrant_error",
        "qdrant upsert batch 0-100 failed for 'nova_knowledge'",
    ), _env("gemini"):
        rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)

    emb = (captured.payload or {}).get("embedding")
    assert emb is not None
    assert (
        emb["last_qdrant_error"]
        == "qdrant upsert batch 0-100 failed for 'nova_knowledge'"
    )


def test_deploy_ready_last_qdrant_error_none_by_default():
    import routes.health as rh

    captured = _CapturingHandler()

    def _fake_send(handler, result, status_code=200):
        captured.payload = result
        captured.status = status_code

    with mock.patch.object(rh, "_send_json_response", _fake_send), mock.patch.object(
        vs, "_last_qdrant_error", None
    ), _env("gemini"):
        rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)

    emb = (captured.payload or {}).get("embedding")
    assert emb is not None
    assert emb["last_qdrant_error"] is None
