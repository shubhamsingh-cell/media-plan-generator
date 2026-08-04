"""Tests for the deterministic Qdrant point ID derivation (F3 fix, 2026-08-04).

Background: vector_search.build_index used to derive each Qdrant point's int
ID via ``abs(hash(entry["id"])) % (2**63)``. Python randomizes str hash()
per-process (no PYTHONHASHSEED pinned anywhere in this repo), so the SAME
doc_id got a DIFFERENT point ID on every deploy -- Qdrant accumulated
duplicate points for the same logical chunk instead of overwriting them in
place. Separately, scripts/reindex_embeddings.py already derived point IDs
deterministically (md5 of doc_id), so the two writers were putting the same
corpus under two disjoint id spaces in the same collection.

The fix factors ONE shared helper, ``vector_search._deterministic_point_id``,
that both build_index and reindex_embeddings.py now call -- no duplicated
derivation left to drift apart.

Runs under pytest, or standalone: ``python3 tests/test_qdrant_point_id.py``.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import vector_search as vs  # noqa: E402


def test_deterministic_point_id_is_stable_across_calls():
    """The same doc_id must yield the same point ID every time -- unlike the
    old abs(hash(...)) derivation, which was randomized per process."""
    doc_id = "joveo_glossary.json:42"
    first = vs._deterministic_point_id(doc_id)
    second = vs._deterministic_point_id(doc_id)
    third = vs._deterministic_point_id(doc_id)
    assert first == second == third


def test_deterministic_point_id_differs_across_doc_ids():
    """Sanity counterpart: different inputs must not collide onto one ID."""
    a = vs._deterministic_point_id("file_a.json:1")
    b = vs._deterministic_point_id("file_b.json:1")
    assert a != b


def test_deterministic_point_id_is_a_valid_int64():
    point_id = vs._deterministic_point_id("some_doc:7")
    assert isinstance(point_id, int)
    assert 0 <= point_id < 2**63


def test_deterministic_point_id_matches_reindex_script_derivation():
    """build_index and scripts/reindex_embeddings.py must resolve the SAME
    doc_id to the SAME point ID -- otherwise a startup index and a manual
    reindex silently maintain two disjoint id spaces for one corpus, which is
    exactly the bug this fix closes.

    reindex_embeddings.py now imports vector_search and calls
    vs._deterministic_point_id directly (no local re-derivation) -- so this
    also guards against a future edit reintroducing a second, divergent
    implementation in the script.
    """
    import importlib.util

    script_path = PROJECT_ROOT / "scripts" / "reindex_embeddings.py"
    spec = importlib.util.spec_from_file_location("reindex_embeddings", script_path)
    reindex_embeddings = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reindex_embeddings)

    doc_id = "joveo_glossary.json:42"
    assert reindex_embeddings.vs is vs
    assert reindex_embeddings.vs._deterministic_point_id(
        doc_id
    ) == vs._deterministic_point_id(doc_id)
    # And it's the actual point-id call site the script's upsert path uses --
    # not a second, shadowing definition.
    import inspect

    source = inspect.getsource(reindex_embeddings)
    assert "def _deterministic_point_id" not in source
    assert "vs._deterministic_point_id(doc[" in source


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


def test_build_index_writes_qdrant_points_via_deterministic_point_id():
    """build_index -- the actual production writer that had this bug, not
    just scripts/reindex_embeddings.py -- must derive each Qdrant point's
    "id" through vs._deterministic_point_id, not the old per-process-
    randomized ``abs(hash(entry["id"])) % (2**63)``.

    Vacuous-test check (see task report): the other 4 tests in this file
    exercise _deterministic_point_id() in isolation and confirm
    reindex_embeddings.py calls it, but none of them ever calls
    vector_search.build_index() -- the writer that actually shipped the bug.
    Reverting vector_search.py's upsert line back to the old
    ``abs(hash(...))`` derivation leaves those 4 tests green. This test
    mocks _qdrant_upsert_points to capture what build_index actually sends
    and asserts the point "id" field, so it fails against that mutation
    (confirmed by running the mutation below).
    """
    captured: dict = {}

    def _fake_urlopen(req, timeout=None, context=None):
        body = {"data": [{"index": 0, "embedding": [0.1] * 1024}]}
        return _FakeResp(body)

    def _fake_upsert(points):
        captured["points"] = points
        return True

    slot_dir = tempfile.mkdtemp(prefix="nova_qdrant_pid_build_index_test_")
    try:
        with mock.patch.dict(
            "os.environ", {"NOVA_SLOT_DIR": slot_dir}, clear=False
        ), mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-4-lite"), mock.patch.object(
            vs, "_VOYAGE_API_KEY", "k"
        ), mock.patch.object(
            vs.urllib.request, "urlopen", side_effect=_fake_urlopen
        ), mock.patch.multiple(
            vs,
            _load_embedding_cache=mock.Mock(),
            _cache_get_locked=mock.Mock(return_value=None),
            _cache_put_locked=mock.Mock(),
            _ensure_flush_thread=mock.Mock(),
            _qdrant_is_configured=mock.Mock(return_value=True),
            _qdrant_ensure_collection=mock.Mock(return_value=True),
            _qdrant_upsert_points=mock.Mock(side_effect=_fake_upsert),
        ), mock.patch.object(
            vs, "_is_startup_indexing", False
        ):
            vs.build_index([{"id": "doc1", "text": "class A driver jobs in Dallas"}])
    finally:
        shutil.rmtree(slot_dir, ignore_errors=True)

    assert "points" in captured, "build_index never reached _qdrant_upsert_points"
    assert len(captured["points"]) == 1
    assert captured["points"][0]["id"] == vs._deterministic_point_id("doc1")


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
