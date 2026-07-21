"""Tests for the Gemini embedding provider path in vector_search (Layer-3 #11).

Covers the EMBEDDING_PROVIDER switch, provider-aware vector dimension
selection, provider-namespaced cache keys, the Gemini batchEmbedContents
request/parse/retry behavior, and graceful failure -- all with the network
mocked (no live API calls). Verifies Voyage stays the default and behaves
exactly as before when EMBEDDING_PROVIDER != "gemini".

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


def test_default_provider_is_voyage():
    with mock.patch.dict("os.environ", {}, clear=False):
        import os

        os.environ.pop("EMBEDDING_PROVIDER", None)
        assert vs.get_embedding_provider() == "voyage"
        assert vs.get_active_embedding_model() == vs._VOYAGE_MODEL
        assert vs._active_vector_dim() == vs._QDRANT_VECTOR_DIM == 512


def test_gemini_provider_selected():
    with _env("gemini"):
        assert vs.get_embedding_provider() == "gemini"
        assert vs.get_active_embedding_model() == "text-embedding-004"
        assert vs._active_vector_dim() == 768


def test_provider_value_is_case_insensitive_and_safe():
    with _env("GEMINI"):
        assert vs.get_embedding_provider() == "gemini"
    with _env("VOYAGE"):
        assert vs.get_embedding_provider() == "voyage"
    # Unknown values fall back to Voyage (never crash, never pick gemini).
    with _env("openai"):
        assert vs.get_embedding_provider() == "voyage"
        assert vs._active_vector_dim() == 512


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
    # and the model must be text-embedding-004.
    sent_req = m.call_args.args[0]
    assert "batchEmbedContents?key=test-key" in sent_req.full_url
    assert "text-embedding-004" in sent_req.full_url


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
    assert st["embedding_model"] == "text-embedding-004"
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
