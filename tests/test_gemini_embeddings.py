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


def test_default_provider_is_gemini():
    """Cutover 2026-07-21: the DEFAULT lives in code (render.yaml envVars are
    not applied by Render for this service -- proven live), so unset env must
    resolve to gemini. Voyage stays reachable as an explicit override, which
    is the no-deploy emergency rollback lever."""
    with mock.patch.dict("os.environ", {}, clear=False):
        import os

        os.environ.pop("EMBEDDING_PROVIDER", None)
        assert vs.get_embedding_provider() == "gemini"
        assert vs.get_active_embedding_model() == vs._GEMINI_EMBED_MODEL


def test_voyage_still_selectable_via_env_override():
    with _env("voyage"):
        assert vs.get_embedding_provider() == "voyage"
        assert vs.get_active_embedding_model() == vs._VOYAGE_MODEL
        assert vs._active_vector_dim() == vs._QDRANT_VECTOR_DIM == 512


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
    # Unknown values resolve to the DEFAULT provider (gemini since the
    # 2026-07-21 cutover) -- never crash, never a half-recognized state.
    with _env("openai"):
        assert vs.get_embedding_provider() == "gemini"
        assert vs._active_vector_dim() == 768


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

    assert captured["body"]["vectors"]["size"] == 512


# ── Qdrant collection is scoped per embedding space ──────────────────────────


def test_active_collection_voyage_is_legacy_name():
    # Voyage keeps the bare legacy name -- it's the collection production has
    # always served from; renaming it would force a needless reindex.
    with _env("voyage"):
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
