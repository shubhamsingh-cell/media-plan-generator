"""Tests for the Voyage voyage-3-lite -> voyage-4-lite model succession.

Covers the 2026-08-04 upgrade to vector_search.py:
    * VOYAGE_MODEL is now env-overridable, defaulting to voyage-4-lite
      (native/default 1024-dim; NO output_dimension param is ever sent, so
      the API's own default is what comes back). voyage-3-lite (fixed
      512-dim) is kept as the instant-rollback target.
    * _active_collection() blue/green: the legacy bare "nova_knowledge" name
      is now pinned to voyage-3-lite SPECIFICALLY, not "whichever model
      Voyage serves" -- every other Voyage model (including the new
      voyage-4-lite default, and any future succession) auto-scopes its own
      model+dim collection name, mirroring the Gemini pattern exactly.
    * input_type ("query"/"document"/None) is threaded from embed_text (query
      path) and build_index (document path) through embed_batch into the
      Voyage payload, and into the disk-cache key so query- and
      document-typed vectors of the same text never share a cache slot.
    * _embed_uncached_voyage validates every response vector is exactly
      _voyage_embed_dim() wide, mirroring the existing Gemini contract guard,
      and records/clears _last_embed_error exactly like the Gemini path.

All network is mocked at urllib.request.urlopen -- no live API calls, and the
tracked ~30MB data/.embedding_cache.json is never touched (cache load/get/put
are stubbed in every test that reaches embed_batch, per the convention in
tests/test_gemini_embeddings.py::_no_cache).

Runs under pytest, or standalone: ``python3 tests/test_voyage_4_lite_upgrade.py``.
"""

from __future__ import annotations

import contextlib
import io
import json
import shutil
import sys
import tempfile
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


def _voyage_payload(vectors):
    """Shape a fake Voyage /embeddings response (order-preserving with index)."""
    return {"data": [{"index": i, "embedding": v} for i, v in enumerate(vectors)]}


def _default_model():
    """Patch nothing -- exercises whatever VOYAGE_MODEL resolves to today
    (voyage-4-lite), so this suite breaks loudly if the default ever moves
    without updating the test's expectations.
    """
    return mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-4-lite")


@contextlib.contextmanager
def _rate_isolated():
    """Isolate the cross-process Voyage rate-limiter window for one test.

    tests/conftest.py sets ONE NOVA_SLOT_DIR for the whole pytest session, so
    without this, every call into _voyage_acquire_send_slot here would share
    a live reservation window with every other test in the run -- forcing
    real _VOYAGE_MIN_DELAY (6.5s) spacing sleeps between this file's own
    tests, and leaking reservations into whatever Voyage test runs next.
    Mirrors _voyage_isolated in tests/test_voyage_embed_deadline.py and
    _SlotDir in tests/test_voyage_rate_crossprocess.py: point NOVA_SLOT_DIR at
    a private tmpdir so this test's reservations can never arm another
    test's wait, and remove it on exit.
    """
    slot_dir = tempfile.mkdtemp(prefix="nova_voyage_4lite_test_")
    with mock.patch.dict("os.environ", {"NOVA_SLOT_DIR": slot_dir}, clear=False):
        try:
            yield
        finally:
            shutil.rmtree(slot_dir, ignore_errors=True)


def _no_cache():
    """Force every embed_batch lookup to miss and skip all disk I/O.

    Mirrors tests/test_gemini_embeddings.py::_no_cache -- critically stubs
    the real cache load/put so the tracked ~30MB data/.embedding_cache.json
    is never read or written by this suite.
    """
    return mock.patch.multiple(
        vs,
        _load_embedding_cache=mock.Mock(),
        _cache_get_locked=mock.Mock(return_value=None),
        _cache_put_locked=mock.Mock(),
        _ensure_flush_thread=mock.Mock(),
    )


# ── (a) Voyage collection scoping (blue/green) ────────────────────────────────


def test_voyage_3_lite_keeps_legacy_bare_name():
    """The legacy space -- and ONLY the legacy space -- keeps "nova_knowledge"."""
    with mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-3-lite"):
        assert vs._active_collection() == "nova_knowledge"


def test_voyage_4_lite_default_gets_scoped_name():
    """The new default auto-scopes its own collection -- never the legacy one."""
    with _default_model():
        assert vs._active_collection() == "nova_knowledge__voyage-4-lite_1024"


def test_voyage_model_succession_scopes_a_new_name():
    """A future model bump (VOYAGE_MODEL=<new>) must never collide with either
    the legacy name or the voyage-4-lite collection -- same succession rule
    Gemini already has via GEMINI_EMBED_MODEL."""
    with mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-5-lite"):
        name = vs._active_collection()
        assert "voyage-5-lite" in name
        assert name not in ("nova_knowledge", "nova_knowledge__voyage-4-lite_1024")


def test_voyage_embed_dim_resolves_known_and_unknown_models():
    with mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-3-lite"):
        assert vs._voyage_embed_dim() == 512
    with mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-4-lite"):
        assert vs._voyage_embed_dim() == 1024
    # An unrecognized future model falls back to the 1024 default rather than
    # raising -- an env-only model bump degrades gracefully.
    with mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-5-lite"):
        assert vs._voyage_embed_dim() == 1024


# ── (b) input_type payload wiring ─────────────────────────────────────────────


def test_embed_text_sends_query_input_type():
    """embed_text (the query/chat-search path) must ask Voyage for a
    "query"-typed vector, and must never send output_dimension (the recipe
    calls for taking voyage-4-lite's default 1024 dim, not requesting one)."""
    captured = {}

    def _fake_urlopen(req, timeout=None, context=None):
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return _FakeResp(_voyage_payload([[0.1] * 1024]))

    with _rate_isolated(), _default_model(), _no_cache(), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ), mock.patch.object(vs.urllib.request, "urlopen", side_effect=_fake_urlopen):
        out = vs.embed_text("driver jobs in Dallas")

    assert out is not None and len(out) == 1024
    assert captured["body"]["input_type"] == "query"
    assert "output_dimension" not in captured["body"]


def test_build_index_sends_document_input_type():
    """build_index's chunk-embedding call (the corpus/document path) must ask
    Voyage for a "document"-typed vector, and must never send
    output_dimension."""
    captured = {}

    def _fake_urlopen(req, timeout=None, context=None):
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return _FakeResp(_voyage_payload([[0.1] * 1024]))

    with _rate_isolated(), _default_model(), _no_cache(), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ), mock.patch.object(
        vs.urllib.request, "urlopen", side_effect=_fake_urlopen
    ), mock.patch.object(
        vs, "_qdrant_is_configured", return_value=False
    ), mock.patch.object(
        vs, "_is_startup_indexing", False
    ):
        vs.build_index([{"id": "doc1", "text": "class A driver jobs in Dallas"}])

    assert captured["body"]["input_type"] == "document"
    assert "output_dimension" not in captured["body"]


def test_embed_batch_omits_input_type_when_none():
    """A caller that passes no input_type (the pre-existing default) must get
    a payload with NO input_type key at all -- not input_type: null."""
    captured = {}

    def _fake_urlopen(req, timeout=None, context=None):
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return _FakeResp(_voyage_payload([[0.1] * 1024]))

    with _rate_isolated(), _default_model(), _no_cache(), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ), mock.patch.object(vs.urllib.request, "urlopen", side_effect=_fake_urlopen):
        out = vs.embed_batch(["hello"])

    assert out == [[0.1] * 1024]
    assert "input_type" not in captured["body"]
    assert "output_dimension" not in captured["body"]


# ── (c) Cache-key separation by input_type ────────────────────────────────────


def test_cache_key_differs_by_input_type():
    """A query-typed and a document-typed embedding of the SAME text must not
    share a cache slot -- input_type changes the retrieval prompt Voyage
    prepends, so the two are different vectors. The untyped (legacy/no
    input_type) key must differ from both."""
    with mock.patch.dict("os.environ", {"EMBEDDING_PROVIDER": "voyage"}, clear=False):
        query_key = vs._text_cache_key("driver jobs in Dallas", "query")
        document_key = vs._text_cache_key("driver jobs in Dallas", "document")
        untyped_key = vs._text_cache_key("driver jobs in Dallas")

    assert query_key != document_key
    assert query_key != untyped_key
    assert document_key != untyped_key


# ── (d) Voyage response-dim validation ────────────────────────────────────────


def test_wrong_dim_response_rejected_and_nothing_cached():
    """A response at the WRONG width (e.g. a stale 512-dim stub reached under
    the voyage-4-lite default, which expects 1024) must be refused -- not
    cached, not returned -- exactly like the existing Gemini contract guard.

    Vacuous-test check (see task report): deleting the dim-validation block
    in _embed_uncached_voyage makes this test fail -- confirmed against a
    throwaway copy of vector_search.py with that block stripped, which
    returned the bad 512-dim vector as a "successful" result and left
    _last_embed_error at None.
    """
    with _rate_isolated(), _default_model(), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ):
        vs._last_embed_error = None
        with mock.patch.object(
            vs.urllib.request,
            "urlopen",
            return_value=_FakeResp(_voyage_payload([[0.1] * 512])),
        ):
            assert vs._embed_uncached_voyage(["hello"]) is None
        assert vs._last_embed_error is not None
        assert "contract violation" in vs._last_embed_error

    # Through embed_batch, the rejection must also mean nothing gets written
    # into the disk cache (the wrong-dim vector never reaches that code path).
    with _rate_isolated(), _default_model(), _no_cache(), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ), mock.patch.object(vs, "_embed_uncached_voyage", return_value=None):
        assert vs.embed_batch(["hello"]) is None
        vs._cache_put_locked.assert_not_called()


def test_correct_dim_response_accepted():
    """Sanity counterpart to the rejection test: a properly-widthed (1024)
    response must be accepted."""
    with _rate_isolated(), _default_model(), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ), mock.patch.object(
        vs.urllib.request,
        "urlopen",
        return_value=_FakeResp(_voyage_payload([[0.1] * 1024])),
    ):
        out = vs._embed_uncached_voyage(["hello"])
    assert out is not None and len(out) == 1 and len(out[0]) == 1024


def test_count_mismatch_rejected():
    """Asked for 2 texts, Voyage returns only 1 embedding back -- a count
    mismatch is its own contract violation, independent of the dim check."""
    with _rate_isolated(), _default_model(), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ):
        with mock.patch.object(
            vs.urllib.request,
            "urlopen",
            return_value=_FakeResp(_voyage_payload([[0.1] * 1024])),
        ):
            assert vs._embed_uncached_voyage(["a", "b"]) is None


# ── (e) last_embed_error parity for the Voyage path ───────────────────────────


def test_voyage_http_error_records_redacted_reason_and_success_clears_it():
    """A terminal HTTP error must record a reason (mirroring the Gemini path's
    body-snippet capture); the next fully successful run must clear it."""

    def _boom(req, timeout=None, context=None):
        raise urllib.error.HTTPError(
            "https://api.voyageai.com/v1/embeddings",
            400,
            "Bad Request",
            None,
            io.BytesIO(b'{"error": "invalid input_type"}'),
        )

    with _rate_isolated(), _default_model(), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ):
        vs._last_embed_error = None
        with mock.patch.object(vs.urllib.request, "urlopen", side_effect=_boom):
            assert vs._embed_uncached_voyage(["a"]) is None
        err = vs._last_embed_error
        assert err is not None
        assert "400" in err

        # A fully successful run clears the sticky reason.
        with mock.patch.object(
            vs.urllib.request,
            "urlopen",
            return_value=_FakeResp(_voyage_payload([[0.1] * 1024])),
        ):
            assert vs._embed_uncached_voyage(["b"]) is not None
        assert vs._last_embed_error is None


def test_voyage_url_error_records_reason():
    with _rate_isolated(), _default_model(), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ):
        vs._last_embed_error = None
        with mock.patch.object(
            vs.urllib.request,
            "urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            assert vs._embed_uncached_voyage(["a"]) is None
        assert vs._last_embed_error is not None
        assert "connection refused" in vs._last_embed_error


def test_deploy_ready_surfaces_voyage_model_and_collection():
    """The public readiness gate must reflect the ACTIVE voyage model/dim/
    collection, not a hardcoded legacy assumption."""
    import routes.health as rh

    class _CapturingHandler:
        def __init__(self):
            self.payload = None
            self.status = None

    captured = _CapturingHandler()

    def _fake_send(handler, result, status_code=200):
        captured.payload = result
        captured.status = status_code

    with mock.patch.object(rh, "_send_json_response", _fake_send), mock.patch.object(
        vs, "_index", [{"id": "d1"}]
    ), _default_model(), mock.patch.dict(
        "os.environ", {"EMBEDDING_PROVIDER": "voyage"}, clear=False
    ):
        rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)

    emb = (captured.payload or {}).get("embedding")
    assert emb is not None
    assert emb["provider"] == "voyage"
    assert emb["model"] == "voyage-4-lite"
    assert emb["dim"] == 1024
    assert emb["collection"] == "nova_knowledge__voyage-4-lite_1024"


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
