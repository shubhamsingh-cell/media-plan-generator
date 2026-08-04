"""rag_pipeline.py -- Nova RAG v2 (Phase 1 promotion).

Top-level module promoted from ``docs/rag_implementation_sketch.py`` per
the Phase 1 plan in ``docs/RAG_Design_2026.md``. The sketch module remains
in place as a tested reference (its 6 pytests re-export from this module).

This is the **runtime** pipeline used by ``nova._query_kb_semantic``. As of
Phase 2 the Qdrant Cloud collection ``nova_knowledge_v2`` is backfilled
(5,153 points, 1024-dim Voyage ``voyage-3.5-lite``, cosine) and callers gate
it behind ``RAG_V2_ENABLED``.

**Transport (Phase 2 REST migration):** The primary embedding + vector-store
path is now **pure stdlib REST** (``urllib`` + ``json`` + ``ssl``) so the
module runs on Render's Python 3.13 build WITHOUT the ``voyageai`` or
``qdrant-client`` pip deps (those wheels were removed in commit 39a12dc;
voyageai's stable wheels cap at Python < 3.13). The SDK-backed classes are
retained as optional fallbacks for environments where the libs happen to be
installed, but ``_make_embedder`` / ``_make_store`` select the REST path
first. Endpoints used:
  - Embeddings: ``POST https://api.voyageai.com/v1/embeddings``
    (Bearer ``VOYAGE_API_KEY``).
  - Rerank: ``POST https://api.voyageai.com/v1/rerank``.
  - Vector query: ``POST {QDRANT_URL}/collections/nova_knowledge_v2/points/query``
    (header ``api-key: QDRANT_API_KEY``), Qdrant REST query API (>= 1.10),
    with a ``/points/search`` fallback for older servers.

Public API (per RAG_Design_2026.md §4):
    - ``NovaRAGPipeline`` -- orchestrator class (``retrieve``, ``index``,
      ``format_for_llm``).
    - ``Document``, ``RetrievalHit`` -- dataclasses for the indexing/
      retrieval contract.
    - ``build_documents_from_kb`` -- helper that walks ``data/*.json``.
    - ``BM25Index`` -- pure-Python BM25 keyword index.

Backward compatibility:
    ``NovaRAG`` is exported as an alias for ``NovaRAGPipeline`` so the
    sketch's existing pytests and any prototype scripts keep working.

Design constraints (per RAG_Design_2026.md §2):
  - Embedding model: Voyage AI ``voyage-3.5-lite`` (1024 dim, Matryoshka)
    with sentence-transformers ``all-MiniLM-L6-v2`` (384 dim) fallback.
  - Vector store: Qdrant Cloud (production) with an in-memory dict fallback
    for local development and CI.
  - Hybrid retrieval: vector + BM25 fused via Reciprocal Rank Fusion.
  - Optional cross-encoder rerank via Voyage ``rerank-2.5-lite``.
  - Structure-aware JSON chunking: one chunk per leaf-ish dict node, with
    a key-path prefix preserved for context.

External dependencies:
  - **None required for the primary path.** Embeddings + vector search run
    over stdlib ``urllib``/``json``/``ssl``.
  - ``voyageai`` -- OPTIONAL fallback (official Voyage SDK). Docs:
    https://docs.voyageai.com/docs/python-sdk
  - ``qdrant-client`` -- OPTIONAL fallback (Qdrant client). Docs:
    https://qdrant.tech/documentation/quick-start/
  - ``sentence-transformers`` -- OPTIONAL local embedding fallback. Docs:
    https://www.sbert.net/
  - ``rank-bm25`` -- not required (we ship a pure-Python BM25 below).

Environment variables consumed:
  - ``VOYAGE_API_KEY``: enables Voyage embeddings (REST). Without it we fall
    back to sentence-transformers, then a test-only hash embedder.
  - ``QDRANT_URL`` / ``QDRANT_API_KEY``: enable the Qdrant Cloud store (REST).
    Without them we use the in-memory dict.
  - ``NOVA_RAG_DATA_DIR``: optional, defaults to ``./data/`` next to this
    module (so the demo runs in the media-plan-generator repo without
    configuration).
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import math
import os
import re
import ssl
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional

# Sibling stdlib-only module (no third-party dep added). Imported at module
# level, not lazily: vector_search.py has zero local-module imports of its
# own (grepped clean), so there is no import cycle to guard against. Needed
# so _Reranker.rerank can reserve from the SAME cross-process, per-model
# Voyage rate window vector_search.py's own rerank caller uses -- Voyage
# meters rerank-2.5-lite per account regardless of which caller sends the
# request, so two independently-throttled callers could jointly over-send.
import vector_search

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stdlib HTTP helper (shared by the Voyage + Qdrant REST backends)
# ---------------------------------------------------------------------------

# Reuse one TLS context across calls (cheap, thread-safe to read).
_SSL_CONTEXT = ssl.create_default_context()

# REST tuning. Timeouts are generous because embedding a batch of 128 or a
# Qdrant query against 5K points can take a few seconds under load.
_HTTP_TIMEOUT_S = 30.0
_HTTP_MAX_RETRIES = 1  # one retry on 429/5xx, per spec
_HTTP_BACKOFF_BASE_S = 1.0


class _RestError(RuntimeError):
    """Raised when a REST call exhausts retries or returns a hard error."""


def _http_post_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    *,
    timeout: float = _HTTP_TIMEOUT_S,
    max_retries: int = _HTTP_MAX_RETRIES,
) -> dict[str, Any]:
    """POST a JSON body and return the parsed JSON response.

    Pure stdlib (urllib + json + ssl). Retries once on HTTP 429 and 5xx with
    exponential backoff (honouring ``Retry-After`` when present). Network
    errors (timeouts, DNS, reset) are retried the same way. 4xx other than
    429 fail fast since a retry won't help.

    Args:
        url: Absolute https URL.
        payload: JSON-serializable request body.
        headers: Request headers (e.g. auth). ``Content-Type`` is forced to
            ``application/json``.
        timeout: Per-attempt socket timeout in seconds.
        max_retries: Number of retries after the first attempt.

    Returns:
        Parsed JSON response as a dict.

    Raises:
        _RestError: On exhausted retries, non-retryable status, or a
            response body that is not a JSON object.
    """
    body = json.dumps(payload).encode("utf-8")
    req_headers = {**headers, "Content-Type": "application/json"}

    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        request = urllib.request.Request(
            url, data=body, headers=req_headers, method="POST"
        )
        try:
            with urllib.request.urlopen(
                request, timeout=timeout, context=_SSL_CONTEXT
            ) as resp:
                raw = resp.read()
            parsed = json.loads(raw.decode("utf-8"))
            if not isinstance(parsed, dict):
                raise _RestError(
                    f"Expected JSON object from {url}, got {type(parsed).__name__}"
                )
            return parsed
        except urllib.error.HTTPError as exc:
            status = exc.code
            retryable = status == 429 or 500 <= status < 600
            # Drain the error body for diagnostics without crashing.
            try:
                detail = exc.read().decode("utf-8", "replace")[:500]
            except Exception:  # noqa: BLE001 -- best effort on error body
                detail = ""
            last_exc = _RestError(f"HTTP {status} from {url}: {detail}")
            if not retryable or attempt >= max_retries:
                logger.error(
                    "POST %s failed (HTTP %s, non-retryable=%s): %s",
                    url,
                    status,
                    not retryable,
                    detail,
                    exc_info=True,
                )
                raise last_exc from exc
            delay = _retry_delay(exc, attempt)
            logger.warning(
                "POST %s -> HTTP %s, retrying in %.1fs (attempt %d/%d)",
                url,
                status,
                delay,
                attempt + 1,
                max_retries,
            )
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt >= max_retries:
                logger.error(
                    "POST %s failed after retries: %s", url, exc, exc_info=True
                )
                raise _RestError(f"POST {url} failed: {exc}") from exc
            delay = _HTTP_BACKOFF_BASE_S * (2**attempt)
            logger.warning(
                "POST %s network error, retrying in %.1fs (attempt %d/%d): %s",
                url,
                delay,
                attempt + 1,
                max_retries,
                exc,
            )
            time.sleep(delay)

    # Unreachable in practice (loop either returns or raises), but keeps the
    # type checker happy and guards against a logic slip.
    raise _RestError(f"POST {url} failed: {last_exc}")


def _retry_delay(exc: urllib.error.HTTPError, attempt: int) -> float:
    """Compute backoff delay, preferring a server ``Retry-After`` header."""
    retry_after = exc.headers.get("Retry-After") if exc.headers else None
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            pass
    return _HTTP_BACKOFF_BASE_S * (2**attempt)


def _http_json(
    url: str,
    headers: dict[str, str],
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = _HTTP_TIMEOUT_S,
    max_retries: int = _HTTP_MAX_RETRIES,
) -> dict[str, Any]:
    """Issue a GET/PUT/POST JSON request (used by the Qdrant REST store).

    Same retry semantics as ``_http_post_json`` (one retry on 429/5xx and
    network errors). A 404 is treated as non-retryable and surfaced so the
    caller can branch (e.g. "collection does not exist").

    Args:
        url: Absolute https URL.
        headers: Request headers (e.g. ``api-key``).
        method: HTTP verb.
        payload: Optional JSON body for PUT/POST.
        timeout: Per-attempt socket timeout.
        max_retries: Retries after the first attempt.

    Returns:
        Parsed JSON response as a dict.

    Raises:
        _RestError: On exhausted retries, non-retryable status, or a
            non-object JSON body.
    """
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req_headers = dict(headers)
    if data is not None:
        req_headers["Content-Type"] = "application/json"

    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        request = urllib.request.Request(
            url, data=data, headers=req_headers, method=method
        )
        try:
            with urllib.request.urlopen(
                request, timeout=timeout, context=_SSL_CONTEXT
            ) as resp:
                raw = resp.read()
            parsed = json.loads(raw.decode("utf-8"))
            if not isinstance(parsed, dict):
                raise _RestError(
                    f"Expected JSON object from {url}, got {type(parsed).__name__}"
                )
            return parsed
        except urllib.error.HTTPError as exc:
            status = exc.code
            retryable = status == 429 or 500 <= status < 600
            try:
                detail = exc.read().decode("utf-8", "replace")[:500]
            except Exception:  # noqa: BLE001 -- best effort on error body
                detail = ""
            last_exc = _RestError(f"HTTP {status} from {url}: {detail}")
            if not retryable or attempt >= max_retries:
                # 404 is an expected control-flow signal for the caller; log
                # at debug, everything else at error.
                log = logger.debug if status == 404 else logger.error
                log(
                    "%s %s -> HTTP %s: %s",
                    method,
                    url,
                    status,
                    detail,
                    exc_info=(status != 404),
                )
                raise last_exc from exc
            delay = _retry_delay(exc, attempt)
            logger.warning(
                "%s %s -> HTTP %s, retrying in %.1fs (attempt %d/%d)",
                method,
                url,
                status,
                delay,
                attempt + 1,
                max_retries,
            )
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt >= max_retries:
                logger.error(
                    "%s %s failed after retries: %s", method, url, exc, exc_info=True
                )
                raise _RestError(f"{method} {url} failed: {exc}") from exc
            delay = _HTTP_BACKOFF_BASE_S * (2**attempt)
            logger.warning(
                "%s %s network error, retrying in %.1fs (attempt %d/%d): %s",
                method,
                url,
                delay,
                attempt + 1,
                max_retries,
                exc,
            )
            time.sleep(delay)

    raise _RestError(f"{method} {url} failed: {last_exc}")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Embedding model selection. voyage-3.5-lite is 1024 dim Matryoshka. We use
# the truncate-to-1024 path. See docs/voyage_4_migration_runbook.md.
_VOYAGE_EMBED_MODEL = "voyage-3.5-lite"
_VOYAGE_EMBED_DIM = 1024
_VOYAGE_RERANK_MODEL = "rerank-2.5-lite"

# Voyage REST endpoints (primary path -- no SDK). Docs:
# https://docs.voyageai.com/reference/embeddings-api
# https://docs.voyageai.com/reference/reranker-api
_VOYAGE_EMBED_URL = "https://api.voyageai.com/v1/embeddings"
_VOYAGE_RERANK_URL = "https://api.voyageai.com/v1/rerank"
# Voyage accepts up to 128 inputs per embeddings call.
_VOYAGE_BATCH_LIMIT = 128
# Per-text char cap to stay comfortably under the model token limit.
_VOYAGE_MAX_CHARS = 32000

# Local fallback. ~80 MB, CPU-friendly, ~5 ms/query.
_LOCAL_EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
_LOCAL_EMBED_DIM = 384

# Qdrant collection. Use v2 suffix so we can ship in parallel with the
# existing ``nova_knowledge`` (685 points) collection.
_QDRANT_COLLECTION = "nova_knowledge_v2"
# Qdrant REST batch size for upserts (keeps the JSON body well under limits).
_QDRANT_UPSERT_BATCH = 100

# Chunking parameters. Tuned in the design doc: 400-800 tokens with 80
# overlap, structure-aware splitting on JSON dict boundaries.
_CHUNK_MIN_TOKENS = 80
_CHUNK_MAX_TOKENS = 800
_CHUNK_OVERLAP_TOKENS = 80

# Retrieval parameters.
_VECTOR_FETCH_K = 20
_BM25_FETCH_K = 20
_RRF_K = 60  # standard RRF constant (Cormack, Clarke, Buettcher 2009)
_RERANK_INPUT_K = 20
_DEFAULT_TOP_K = 5
_RRF_SCORE_FLOOR = 0.015  # empirical noise floor; drop chunks below this


# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Document:
    """A single chunk of indexable text.

    Args:
        doc_id: Deterministic UUID built from (source_file, kb_section,
            chunk_index) so re-indexing is idempotent.
        text: The chunk text, already truncated to ``_CHUNK_MAX_TOKENS``.
        metadata: Filterable payload (country, metric, year, vertical, ...).
            Schema documented in RAG_Design_2026.md §2.3.
    """

    doc_id: str
    text: str
    metadata: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class RetrievalHit:
    """A retrieved chunk with provenance."""

    doc_id: str
    text: str
    metadata: dict[str, Any]
    score: float
    source_file: str
    search_method: str  # "vector" | "bm25" | "hybrid_rrf" | "rerank"


# ---------------------------------------------------------------------------
# Embedding backends
# ---------------------------------------------------------------------------


class _EmbeddingBackend:
    """Abstract base. Subclasses implement ``embed_batch`` and ``dim``."""

    name: str = "abstract"
    dim: int = 0

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError


class VoyageEmbedder(_EmbeddingBackend):
    """Voyage AI embedder. Requires VOYAGE_API_KEY.

    Docs: https://docs.voyageai.com/docs/embeddings
    SDK: https://docs.voyageai.com/docs/python-sdk
    """

    name = "voyage-3.5-lite"
    dim = _VOYAGE_EMBED_DIM

    def __init__(self, api_key: str | None = None) -> None:
        try:
            import voyageai  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "voyageai package not installed. Run `pip install voyageai`."
            ) from exc
        key = api_key or os.environ.get("VOYAGE_API_KEY") or ""
        if not key:
            raise RuntimeError(
                "VOYAGE_API_KEY not set. Sign up at https://www.voyageai.com."
            )
        self._client = voyageai.Client(api_key=key)

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts. Voyage allows up to 128 per call."""
        if not texts:
            return []
        # Truncate each text to ~32K chars to stay under model token limit.
        truncated = [t[:32000] for t in texts]
        # input_type="document" for indexing, "query" for retrieval -- Voyage
        # uses asymmetric encoders.
        result = self._client.embed(
            truncated, model=_VOYAGE_EMBED_MODEL, input_type="document"
        )
        return [list(v) for v in result.embeddings]

    def embed_query(self, query: str) -> list[float]:
        """Single-query embedding with input_type='query' (asymmetric)."""
        result = self._client.embed(
            [query[:32000]], model=_VOYAGE_EMBED_MODEL, input_type="query"
        )
        return list(result.embeddings[0])


class VoyageRESTEmbedder(_EmbeddingBackend):
    """Voyage AI embedder over stdlib REST. Requires VOYAGE_API_KEY.

    This is the **primary** production embedder: it has zero pip
    dependencies (urllib + json only), so it works on Render's Python 3.13
    build where the ``voyageai`` SDK wheel is unavailable.

    POSTs to ``https://api.voyageai.com/v1/embeddings`` with body
    ``{"input": [...], "model": "voyage-3.5-lite", "input_type": ...}`` and
    parses ``data[].embedding``. Voyage uses asymmetric encoders, so we send
    ``input_type="document"`` when indexing and ``"query"`` when retrieving.

    Docs: https://docs.voyageai.com/reference/embeddings-api
    """

    name = "voyage-3.5-lite-rest"
    dim = _VOYAGE_EMBED_DIM

    def __init__(self, api_key: str | None = None) -> None:
        key = api_key or os.environ.get("VOYAGE_API_KEY") or ""
        if not key:
            raise RuntimeError(
                "VOYAGE_API_KEY not set. Sign up at https://www.voyageai.com."
            )
        self._api_key = key

    def _embed(self, texts: list[str], input_type: str) -> list[list[float]]:
        """POST one batch (<=128 inputs) and return embeddings in order.

        Args:
            texts: Inputs for a single API call. Must be <= 128 items.
            input_type: ``"document"`` (indexing) or ``"query"`` (retrieval).

        Returns:
            Embedding vectors parallel to ``texts``.

        Raises:
            _RestError: On a failed REST call or a malformed response.
        """
        if not texts:
            return []
        payload = {
            "input": [t[:_VOYAGE_MAX_CHARS] for t in texts],
            "model": _VOYAGE_EMBED_MODEL,
            "input_type": input_type,
        }
        headers = {"Authorization": f"Bearer {self._api_key}"}
        data = _http_post_json(_VOYAGE_EMBED_URL, payload, headers)
        rows = data.get("data")
        if not isinstance(rows, list):
            raise _RestError(f"Voyage response missing 'data' list: {str(data)[:200]}")
        # Voyage returns rows with an "index" field; sort defensively so the
        # output order always matches the input order.
        try:
            rows_sorted = sorted(rows, key=lambda r: int(r.get("index", 0)))
        except (TypeError, ValueError):
            rows_sorted = rows
        embeddings: list[list[float]] = []
        for row in rows_sorted:
            emb = row.get("embedding")
            if not isinstance(emb, list):
                raise _RestError("Voyage row missing 'embedding' list.")
            embeddings.append([float(x) for x in emb])
        if len(embeddings) != len(texts):
            raise _RestError(
                f"Voyage returned {len(embeddings)} vectors for {len(texts)} inputs."
            )
        return embeddings

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed texts for indexing (input_type='document').

        Splits into <=128-input calls per the Voyage batch limit.
        """
        if not texts:
            return []
        out: list[list[float]] = []
        for i in range(0, len(texts), _VOYAGE_BATCH_LIMIT):
            out.extend(self._embed(texts[i : i + _VOYAGE_BATCH_LIMIT], "document"))
        return out

    def embed_query(self, query: str) -> list[float]:
        """Single-query embedding with input_type='query' (asymmetric)."""
        return self._embed([query], "query")[0]


class LocalEmbedder(_EmbeddingBackend):
    """sentence-transformers fallback. CPU-only, no API key needed.

    Docs: https://www.sbert.net/docs/quickstart.html
    """

    name = "all-MiniLM-L6-v2"
    dim = _LOCAL_EMBED_DIM

    def __init__(self) -> None:
        try:
            from sentence_transformers import (  # type: ignore[import-not-found]
                SentenceTransformer,
            )
        except ImportError as exc:
            raise RuntimeError(
                "sentence-transformers not installed. "
                "Run `pip install sentence-transformers`."
            ) from exc
        self._model = SentenceTransformer(_LOCAL_EMBED_MODEL)

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        vecs = self._model.encode(
            texts, batch_size=32, show_progress_bar=False, normalize_embeddings=True
        )
        return [list(map(float, v)) for v in vecs]

    def embed_query(self, query: str) -> list[float]:
        return self.embed_batch([query])[0]


class HashEmbedder(_EmbeddingBackend):
    """Deterministic hashing embedder. Used ONLY for tests and the demo.

    This is not a real embedding -- it's a stable, fast, content-addressed
    pseudo-vector so the demo and pytest can run without any network or
    model download. It will not give good retrieval quality. The production
    code should never hit this path; we raise an explicit warning when it
    is constructed outside of a test context.
    """

    name = "hash-deterministic-test-only"
    dim = 128

    def __init__(self) -> None:
        logger.warning(
            "HashEmbedder is for tests/demos only; quality will be poor. "
            "Install voyageai or sentence-transformers for real retrieval."
        )

    def _hash_vector(self, text: str) -> list[float]:
        """Map text -> 128-dim unit vector via SHA256 byte chunks."""
        h = hashlib.sha256(text.lower().encode("utf-8")).digest()
        # Repeat the hash bytes until we have 128 ints, then L2-normalize.
        raw = (h * ((self.dim // len(h)) + 1))[: self.dim]
        vec = [(b - 128) / 128.0 for b in raw]
        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        return [x / norm for x in vec]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self._hash_vector(t) for t in texts]

    def embed_query(self, query: str) -> list[float]:
        return self._hash_vector(query)


def _make_embedder(prefer: str | None = None) -> _EmbeddingBackend:
    """Select the best available embedder.

    Priority (auto):
        1. ``prefer`` argument if provided ("voyage_rest" | "voyage" |
           "local" | "hash").
        2. Voyage via stdlib REST if VOYAGE_API_KEY is set (PRIMARY -- no
           pip deps).
        3. Voyage SDK if VOYAGE_API_KEY is set and ``voyageai`` is importable.
        4. sentence-transformers if importable.
        5. HashEmbedder (test-only fallback).

    Args:
        prefer: Explicit backend choice for tests. ``"voyage"`` keeps the
            historical SDK meaning; ``"voyage_rest"`` forces the REST path.

    Returns:
        An embedder instance.
    """
    if prefer == "voyage_rest":
        return VoyageRESTEmbedder()
    if prefer == "voyage":
        return VoyageEmbedder()
    if prefer == "local":
        return LocalEmbedder()
    if prefer == "hash":
        return HashEmbedder()

    if os.environ.get("VOYAGE_API_KEY"):
        # Primary: REST (works without the voyageai wheel on Python 3.13).
        try:
            return VoyageRESTEmbedder()
        except RuntimeError as exc:
            logger.warning("Voyage REST unavailable, trying SDK: %s", exc)
        # Optional: SDK, only if the lib happens to be installed.
        try:
            return VoyageEmbedder()
        except RuntimeError as exc:
            logger.warning("Voyage SDK unavailable, falling back: %s", exc)
    try:
        return LocalEmbedder()
    except RuntimeError as exc:
        logger.warning("Local embedder unavailable, falling back to hash: %s", exc)
    return HashEmbedder()


# ---------------------------------------------------------------------------
# Vector store backends
# ---------------------------------------------------------------------------


class _VectorStore:
    """Abstract base for vector stores. Subclasses implement upsert/search."""

    def upsert(self, docs: list[Document], vectors: list[list[float]]) -> int:
        raise NotImplementedError

    def search(
        self,
        query_vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[str, float]]:
        raise NotImplementedError

    def search_payload(
        self,
        query_vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[str, float, Document]] | None:
        """Optional: search returning hydrated Documents in one round trip.

        Backends that can return the payload alongside the score (e.g.
        Qdrant ``with_payload=true``) override this so the orchestrator can
        avoid an N+1 ``get()`` per candidate. Returning ``None`` signals
        "not supported -- fall back to ``search`` + ``get``".
        """
        return None

    def get(self, doc_id: str) -> Document | None:
        raise NotImplementedError

    def count(self) -> int:
        raise NotImplementedError


class QdrantStore(_VectorStore):
    """Qdrant Cloud-backed store.

    Docs: https://qdrant.tech/documentation/quick-start/
    Filtering: https://qdrant.tech/documentation/concepts/filtering/
    Python client: https://github.com/qdrant/qdrant-client
    """

    def __init__(
        self,
        url: str | None = None,
        api_key: str | None = None,
        collection: str = _QDRANT_COLLECTION,
        dim: int = _VOYAGE_EMBED_DIM,
    ) -> None:
        try:
            from qdrant_client import QdrantClient  # type: ignore[import-not-found]
            from qdrant_client.http import models as qmodels  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "qdrant-client not installed. Run `pip install qdrant-client`."
            ) from exc

        self._qmodels = qmodels
        url = url or os.environ.get("QDRANT_URL") or ""
        api_key = api_key or os.environ.get("QDRANT_API_KEY") or ""
        if not url or not api_key:
            raise RuntimeError("QDRANT_URL and QDRANT_API_KEY required.")

        self._client = QdrantClient(url=url, api_key=api_key, timeout=15)
        self._collection = collection
        self._dim = dim
        self._ensure_collection()

    def _ensure_collection(self) -> None:
        """Create the collection if missing, idempotent."""
        existing = {c.name for c in self._client.get_collections().collections}
        if self._collection in existing:
            return
        self._client.create_collection(
            collection_name=self._collection,
            vectors_config=self._qmodels.VectorParams(
                size=self._dim, distance=self._qmodels.Distance.COSINE
            ),
        )
        # Add payload indexes for fast filtering.
        for field in ("source_file", "country", "metric", "year", "vertical"):
            try:
                self._client.create_payload_index(
                    collection_name=self._collection,
                    field_name=field,
                    field_schema=self._qmodels.PayloadSchemaType.KEYWORD,
                )
            except Exception as exc:  # noqa: BLE001 -- best effort
                logger.warning("Payload index for %s failed: %s", field, exc)

    def upsert(self, docs: list[Document], vectors: list[list[float]]) -> int:
        if not docs:
            return 0
        if len(docs) != len(vectors):
            raise ValueError("docs and vectors length mismatch.")
        points = [
            self._qmodels.PointStruct(
                id=doc.doc_id,
                vector=vec,
                payload={**doc.metadata, "text": doc.text},
            )
            for doc, vec in zip(docs, vectors)
        ]
        batch_size = 100
        total = 0
        for i in range(0, len(points), batch_size):
            self._client.upsert(
                collection_name=self._collection, points=points[i : i + batch_size]
            )
            total += len(points[i : i + batch_size])
        return total

    def _build_filter(self, filters: dict[str, Any] | None):
        if not filters:
            return None
        must = []
        for key, value in filters.items():
            if isinstance(value, list):
                must.append(
                    self._qmodels.FieldCondition(
                        key=key, match=self._qmodels.MatchAny(any=value)
                    )
                )
            else:
                must.append(
                    self._qmodels.FieldCondition(
                        key=key, match=self._qmodels.MatchValue(value=value)
                    )
                )
        return self._qmodels.Filter(must=must)

    def search(
        self,
        query_vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[str, float]]:
        """Vector search with cross-version qdrant-client compatibility.

        The qdrant-client API renamed ``search`` -> ``query_points`` in
        v1.10. We prefer the newer API and fall back to ``search`` so the
        same code runs against both Render (whatever pip resolves) and
        local development environments.
        """
        qfilter = self._build_filter(filters)

        # New API (qdrant-client >= 1.10): client.query_points returns a
        # QueryResponse with a ``.points`` list of ScoredPoint.
        query_points = getattr(self._client, "query_points", None)
        if callable(query_points):
            response = query_points(
                collection_name=self._collection,
                query=query_vector,
                query_filter=qfilter,
                limit=top_k,
                with_payload=False,
                with_vectors=False,
            )
            points = getattr(response, "points", response)
            return [(str(p.id), float(p.score)) for p in points]

        # Legacy API (qdrant-client < 1.10): client.search.
        results = self._client.search(
            collection_name=self._collection,
            query_vector=query_vector,
            query_filter=qfilter,
            limit=top_k,
            with_payload=False,
            with_vectors=False,
        )
        return [(str(r.id), float(r.score)) for r in results]

    def get(self, doc_id: str) -> Document | None:
        result = self._client.retrieve(
            collection_name=self._collection, ids=[doc_id], with_payload=True
        )
        if not result:
            return None
        point = result[0]
        payload = dict(point.payload or {})
        text = payload.pop("text", "")
        return Document(doc_id=str(point.id), text=text, metadata=payload)

    def count(self) -> int:
        info = self._client.get_collection(collection_name=self._collection)
        return int(info.points_count or 0)


class QdrantRESTStore(_VectorStore):
    """Qdrant Cloud-backed store over stdlib REST (PRIMARY production path).

    Zero pip dependencies (urllib + json only) so it runs on Render's Python
    3.13 build without the ``qdrant-client`` wheel. Talks to the Qdrant REST
    API directly:

      - Query: ``POST {url}/collections/{c}/points/query`` (>= 1.10). Falls
        back to the legacy ``POST .../points/search`` if the server returns
        404/Not Implemented for ``/query``.
      - Retrieve: ``POST .../points`` with ``{"ids": [...]}``.
      - Upsert: ``PUT .../points`` with a ``{"points": [...]}`` body.
      - Count: ``GET .../collections/{c}`` -> ``result.points_count``.

    The Phase 2 backfill already populated this collection (5,153 points),
    so in production only ``search``/``get``/``count`` are exercised; the
    ``upsert`` path exists for re-indexing and is exercised by the backfill
    tooling.

    Docs:
      - Query API: https://api.qdrant.tech/api-reference/search/query-points
      - Filtering: https://qdrant.tech/documentation/concepts/filtering/
    """

    def __init__(
        self,
        url: str | None = None,
        api_key: str | None = None,
        collection: str = _QDRANT_COLLECTION,
        dim: int = _VOYAGE_EMBED_DIM,
    ) -> None:
        url = (url or os.environ.get("QDRANT_URL") or "").rstrip("/")
        api_key = api_key or os.environ.get("QDRANT_API_KEY") or ""
        if not url or not api_key:
            raise RuntimeError("QDRANT_URL and QDRANT_API_KEY required.")
        self._base = url
        self._api_key = api_key
        self._collection = collection
        self._dim = dim
        # Sticky flag so we don't probe ``/points/query`` on every search
        # once we've learned the server only supports the legacy endpoint.
        self._use_legacy_search = False

    # ----- helpers ---------------------------------------------------------

    @property
    def _headers(self) -> dict[str, str]:
        return {"api-key": self._api_key}

    def _coll_url(self, suffix: str = "") -> str:
        return f"{self._base}/collections/{self._collection}{suffix}"

    @staticmethod
    def _build_filter(filters: dict[str, Any] | None) -> dict[str, Any] | None:
        """Translate ``{key: value}`` / ``{key: [v1, v2]}`` to a Qdrant filter.

        Produces the REST JSON shape::

            {"must": [{"key": "country", "match": {"value": "US"}}, ...]}

        List values use ``{"match": {"any": [...]}}``.
        """
        if not filters:
            return None
        must: list[dict[str, Any]] = []
        for key, value in filters.items():
            if isinstance(value, list):
                must.append({"key": key, "match": {"any": value}})
            else:
                must.append({"key": key, "match": {"value": value}})
        return {"must": must}

    # ----- write path (re-indexing / backfill) ----------------------------

    def _ensure_collection(self) -> None:
        """Create the collection + payload indexes if missing (idempotent).

        Production never needs this (the backfill created the collection),
        so it is only called from ``upsert`` to keep re-indexing self-healing.
        """
        try:
            _http_json(self._coll_url(), self._headers, method="GET")
            return  # already exists
        except _RestError as exc:
            if "HTTP 404" not in str(exc):
                raise
        # Create the collection.
        _http_json(
            self._coll_url(),
            self._headers,
            method="PUT",
            payload={
                "vectors": {"size": self._dim, "distance": "Cosine"},
            },
        )
        # Best-effort payload indexes for fast filtering.
        for field in ("source_file", "country", "metric", "year", "vertical"):
            try:
                _http_json(
                    self._coll_url("/index"),
                    self._headers,
                    method="PUT",
                    payload={"field_name": field, "field_schema": "keyword"},
                )
            except _RestError as exc:  # missing index only slows queries
                logger.warning("Payload index for %s failed: %s", field, exc)

    def upsert(self, docs: list[Document], vectors: list[list[float]]) -> int:
        if not docs:
            return 0
        if len(docs) != len(vectors):
            raise ValueError("docs and vectors length mismatch.")
        self._ensure_collection()
        points = [
            {
                "id": doc.doc_id,
                "vector": vec,
                "payload": {**doc.metadata, "text": doc.text},
            }
            for doc, vec in zip(docs, vectors)
        ]
        total = 0
        for i in range(0, len(points), _QDRANT_UPSERT_BATCH):
            batch = points[i : i + _QDRANT_UPSERT_BATCH]
            _http_json(
                self._coll_url("/points"),
                self._headers,
                method="PUT",
                payload={"points": batch},
            )
            total += len(batch)
        return total

    # ----- read path (production) ------------------------------------------

    def _search_query_api(
        self,
        query_vector: list[float],
        top_k: int,
        qfilter: dict[str, Any] | None,
    ) -> list[tuple[str, float]]:
        """Current ``/points/query`` API (Qdrant >= 1.10)."""
        payload: dict[str, Any] = {
            "query": query_vector,
            "limit": top_k,
            "with_payload": False,
            "with_vector": False,
        }
        if qfilter:
            payload["filter"] = qfilter
        data = _http_post_json(self._coll_url("/points/query"), payload, self._headers)
        points = data.get("result", {}).get("points", [])
        return [(str(p.get("id")), float(p.get("score", 0.0))) for p in points]

    def _search_legacy_api(
        self,
        query_vector: list[float],
        top_k: int,
        qfilter: dict[str, Any] | None,
    ) -> list[tuple[str, float]]:
        """Legacy ``/points/search`` API (Qdrant < 1.10)."""
        payload: dict[str, Any] = {
            "vector": query_vector,
            "limit": top_k,
            "with_payload": False,
            "with_vector": False,
        }
        if qfilter:
            payload["filter"] = qfilter
        data = _http_post_json(self._coll_url("/points/search"), payload, self._headers)
        results = data.get("result", [])
        return [(str(r.get("id")), float(r.get("score", 0.0))) for r in results]

    def search(
        self,
        query_vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[str, float]]:
        """Vector search via REST with query->search endpoint fallback."""
        qfilter = self._build_filter(filters)
        if not self._use_legacy_search:
            try:
                return self._search_query_api(query_vector, top_k, qfilter)
            except _RestError as exc:
                # Older servers lack /points/query (404 / 501). Latch to the
                # legacy endpoint so we only pay the failed probe once.
                if "HTTP 404" in str(exc) or "HTTP 501" in str(exc):
                    logger.warning(
                        "Qdrant /points/query unavailable, using legacy "
                        "/points/search: %s",
                        exc,
                    )
                    self._use_legacy_search = True
                else:
                    raise
        return self._search_legacy_api(query_vector, top_k, qfilter)

    @staticmethod
    def _point_to_doc(point: dict[str, Any]) -> Document:
        """Build a Document from a Qdrant point's id + payload."""
        payload = dict(point.get("payload") or {})
        text = payload.pop("text", "")
        return Document(doc_id=str(point.get("id")), text=text, metadata=payload)

    def search_payload(
        self,
        query_vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[str, float, Document]] | None:
        """Search returning ``(doc_id, score, Document)`` in one REST call.

        Uses ``with_payload=true`` so the orchestrator avoids an N+1 ``get``
        per candidate -- the production hot path, where the in-process doc
        mirror is empty because the corpus lives only in Qdrant. Falls back
        to the base-class ``None`` contract if the endpoint errors, so
        ``retrieve`` can degrade to ``search`` + ``get``.
        """
        qfilter = self._build_filter(filters)
        endpoint = "/points/search" if self._use_legacy_search else "/points/query"
        is_query_api = not self._use_legacy_search
        payload: dict[str, Any] = {
            "limit": top_k,
            "with_payload": True,
            "with_vector": False,
        }
        if is_query_api:
            payload["query"] = query_vector
        else:
            payload["vector"] = query_vector
        if qfilter:
            payload["filter"] = qfilter
        try:
            data = _http_post_json(self._coll_url(endpoint), payload, self._headers)
        except _RestError as exc:
            if is_query_api and ("HTTP 404" in str(exc) or "HTTP 501" in str(exc)):
                logger.warning(
                    "Qdrant /points/query unavailable, using legacy "
                    "/points/search: %s",
                    exc,
                )
                self._use_legacy_search = True
                return self.search_payload(query_vector, top_k, filters)
            logger.warning("Qdrant search_payload failed: %s", exc)
            return None
        result = data.get("result", {})
        points = result.get("points", []) if is_query_api else result
        out: list[tuple[str, float, Document]] = []
        for p in points:
            out.append(
                (str(p.get("id")), float(p.get("score", 0.0)), self._point_to_doc(p))
            )
        return out

    def get(self, doc_id: str) -> Document | None:
        try:
            data = _http_post_json(
                self._coll_url("/points"),
                {"ids": [doc_id], "with_payload": True, "with_vector": False},
                self._headers,
            )
        except _RestError as exc:
            logger.warning("Qdrant get(%s) failed: %s", doc_id, exc)
            return None
        result = data.get("result", [])
        if not result:
            return None
        point = result[0]
        payload = dict(point.get("payload") or {})
        text = payload.pop("text", "")
        return Document(doc_id=str(point.get("id")), text=text, metadata=payload)

    def count(self) -> int:
        try:
            data = _http_json(self._coll_url(), self._headers, method="GET")
        except _RestError as exc:
            logger.warning("Qdrant count failed: %s", exc)
            return 0
        return int(data.get("result", {}).get("points_count") or 0)


class InMemoryStore(_VectorStore):
    """In-process vector store. For tests, local dev, and Qdrant outages.

    Stores docs + vectors in a dict. Cosine similarity via dot product on
    normalized vectors. Not threadsafe; wrap in a lock if shared.
    """

    def __init__(self) -> None:
        self._docs: dict[str, Document] = {}
        self._vecs: dict[str, list[float]] = {}

    @staticmethod
    def _normalize(v: list[float]) -> list[float]:
        norm = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / norm for x in v]

    def upsert(self, docs: list[Document], vectors: list[list[float]]) -> int:
        for doc, vec in zip(docs, vectors):
            self._docs[doc.doc_id] = doc
            self._vecs[doc.doc_id] = self._normalize(vec)
        return len(docs)

    @staticmethod
    def _passes_filter(doc: Document, filters: dict[str, Any] | None) -> bool:
        if not filters:
            return True
        for k, v in filters.items():
            doc_v = doc.metadata.get(k)
            if isinstance(v, list):
                if doc_v not in v:
                    return False
            else:
                if doc_v != v:
                    return False
        return True

    def search(
        self,
        query_vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[str, float]]:
        qv = self._normalize(query_vector)
        scored: list[tuple[float, str]] = []
        for doc_id, vec in self._vecs.items():
            doc = self._docs.get(doc_id)
            if doc is None or not self._passes_filter(doc, filters):
                continue
            sim = sum(a * b for a, b in zip(qv, vec))
            scored.append((sim, doc_id))
        scored.sort(reverse=True)
        return [(doc_id, score) for score, doc_id in scored[:top_k]]

    def get(self, doc_id: str) -> Document | None:
        return self._docs.get(doc_id)

    def count(self) -> int:
        return len(self._docs)


def _make_store(
    prefer: str | None = None, dim: int = _VOYAGE_EMBED_DIM
) -> _VectorStore:
    """Select the best available vector store.

    Priority (auto):
        1. ``prefer`` argument if provided ("qdrant_rest" | "qdrant" |
           "memory").
        2. Qdrant via stdlib REST if URL + key set (PRIMARY -- no pip deps).
        3. Qdrant SDK if URL + key set and ``qdrant-client`` is importable.
        4. InMemory fallback.

    Args:
        prefer: Explicit backend choice. ``"qdrant"`` keeps the historical
            SDK meaning; ``"qdrant_rest"`` forces the REST path.
        dim: Vector dimensionality (used only when creating a collection).
    """
    if prefer == "qdrant_rest":
        return QdrantRESTStore(dim=dim)
    if prefer == "qdrant":
        return QdrantStore(dim=dim)
    if prefer == "memory":
        return InMemoryStore()

    if os.environ.get("QDRANT_URL") and os.environ.get("QDRANT_API_KEY"):
        # Primary: REST (works without the qdrant-client wheel on Py 3.13).
        try:
            return QdrantRESTStore(dim=dim)
        except RuntimeError as exc:
            logger.warning("Qdrant REST unavailable, trying SDK: %s", exc)
        # Optional: SDK, only if the lib happens to be installed.
        try:
            return QdrantStore(dim=dim)
        except RuntimeError as exc:
            logger.warning("Qdrant SDK unavailable, falling back: %s", exc)
    return InMemoryStore()


# ---------------------------------------------------------------------------
# BM25 keyword index (pure Python)
# ---------------------------------------------------------------------------


# Stop words copied verbatim from vector_search.py for behavior parity.
_STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "the",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "is",
        "it",
        "as",
        "was",
        "are",
        "be",
        "has",
        "had",
        "have",
        "this",
        "that",
        "these",
        "those",
        "not",
        "no",
        "will",
        "can",
        "do",
        "if",
        "so",
        "than",
        "too",
        "very",
        "just",
    }
)
_TOKEN_RE = re.compile(r"[A-Za-z0-9_\-]+")


def _tokenize(text: str) -> list[str]:
    """Tokenize: lowercase, drop stop words, single chars, and numerics-only."""
    return [
        t.lower()
        for t in _TOKEN_RE.findall(text)
        if len(t) > 1 and t.lower() not in _STOP_WORDS
    ]


class BM25Index:
    """Okapi BM25 keyword index. Matches vector_search.BM25Index contract.

    Args:
        k1: Term frequency saturation parameter.
        b: Document length normalization parameter.
    """

    def __init__(self, k1: float = 1.5, b: float = 0.75) -> None:
        self.k1 = k1
        self.b = b
        self._doc_ids: list[str] = []
        self._tf: list[dict[str, int]] = []
        self._df: dict[str, int] = {}
        self._dl: list[int] = []
        self._avg_dl: float = 0.0
        self._N: int = 0
        self._built = False

    def index(self, docs: list[Document]) -> None:
        # Reset internal state on each rebuild so callers can re-index.
        self._doc_ids = []
        self._tf = []
        self._dl = []
        df: dict[str, int] = defaultdict(int)
        for doc in docs:
            tokens = _tokenize(doc.text)
            tf = dict(Counter(tokens))
            self._doc_ids.append(doc.doc_id)
            self._tf.append(tf)
            self._dl.append(len(tokens))
            for t in set(tokens):
                df[t] += 1
        self._df = dict(df)
        self._N = len(docs)
        self._avg_dl = sum(self._dl) / self._N if self._N else 0.0
        self._built = True

    def search(self, query: str, top_k: int) -> list[tuple[str, float]]:
        if not self._built or self._N == 0:
            return []
        q_tokens = _tokenize(query)
        if not q_tokens:
            return []
        scores: list[tuple[float, int]] = []
        for i in range(self._N):
            score = 0.0
            for term in q_tokens:
                df_t = self._df.get(term)
                if not df_t:
                    continue
                f = self._tf[i].get(term, 0)
                if not f:
                    continue
                idf = math.log((self._N - df_t + 0.5) / (df_t + 0.5) + 1.0)
                dl = self._dl[i]
                tf_norm = (f * (self.k1 + 1.0)) / (
                    f + self.k1 * (1.0 - self.b + self.b * dl / (self._avg_dl or 1))
                )
                score += idf * tf_norm
            if score > 0:
                scores.append((score, i))
        scores.sort(reverse=True)
        return [(self._doc_ids[idx], round(s, 4)) for s, idx in scores[:top_k]]


# ---------------------------------------------------------------------------
# Reranker (cross-encoder via Voyage rerank API)
# ---------------------------------------------------------------------------


class _Reranker:
    """Cross-encoder rerank wrapper. Voyage rerank-2.5-lite via stdlib REST.

    Primary path POSTs to ``https://api.voyageai.com/v1/rerank`` with body
    ``{"query": ..., "documents": [...], "model": "rerank-2.5-lite",
    "top_k": k}`` and parses ``data[].{index, relevance_score}``. No pip
    deps. Rerank is a best-effort quality boost: on any failure (missing
    key, REST error, malformed response) we fall back to the RRF ordering so
    retrieval never breaks.
    """

    def __init__(self, enabled: bool = True) -> None:
        self._api_key = os.environ.get("VOYAGE_API_KEY") or ""
        self._enabled = bool(enabled and self._api_key)

    def rerank(
        self, query: str, candidates: list[RetrievalHit], top_k: int
    ) -> list[RetrievalHit]:
        """Return top_k candidates re-ordered by cross-encoder score.

        Args:
            query: User query.
            candidates: Hits from RRF fusion.
            top_k: Number of hits to return.

        Returns:
            Reranked hits. Falls back to RRF input order if rerank fails, or
            if the shared per-model Voyage rate window is full (see below).
        """
        if not self._enabled or not candidates or len(candidates) <= 1:
            return candidates[:top_k]
        # Reserve from rerank-2.5-lite's cross-process, per-model window
        # BEFORE spending a request -- the same window vector_search.py's
        # own rerank caller reserves from (_VOYAGE_WINDOW_RERANK), since
        # Voyage meters each model per-account, not per-caller. Rerank is
        # best-effort with an RRF-order fallback, so a full window (or a
        # contended lock) declines here rather than risk a 429, mirroring
        # vector_search.py's own pre-POST rerank rate-limit check.
        if not vector_search._voyage_try_reserve_slot():
            logger.info("Rerank shared rate window full, returning RRF order")
            return candidates[:top_k]
        try:
            documents = [c.text[:2000] for c in candidates]
            payload = {
                "query": query,
                "documents": documents,
                "model": _VOYAGE_RERANK_MODEL,
                "top_k": top_k,
            }
            headers = {"Authorization": f"Bearer {self._api_key}"}
            data = _http_post_json(_VOYAGE_RERANK_URL, payload, headers)
            rows = data.get("data")
            if not isinstance(rows, list) or not rows:
                raise _RestError(f"Voyage rerank missing 'data': {str(data)[:200]}")
            ranked: list[RetrievalHit] = []
            for row in rows:
                idx = int(row.get("index", -1))
                if idx < 0 or idx >= len(candidates):
                    continue
                ranked.append(
                    dataclasses.replace(
                        candidates[idx],
                        score=float(row.get("relevance_score", 0.0)),
                        search_method="rerank",
                    )
                )
            return ranked or candidates[:top_k]
        except (_RestError, KeyError, TypeError, ValueError) as exc:
            logger.warning("Rerank failed, returning RRF order: %s", exc)
            return candidates[:top_k]


# ---------------------------------------------------------------------------
# Chunking and metadata extraction
# ---------------------------------------------------------------------------


_COUNTRY_PATTERNS = {
    "US": re.compile(r"\b(united states|usa|america)\b", re.I),
    "UK": re.compile(r"\b(united kingdom|britain|england)\b", re.I),
    "CA": re.compile(r"\b(canada|canadian)\b", re.I),
    "DE": re.compile(r"\b(germany|deutschland)\b", re.I),
    "FR": re.compile(r"\b(france|french)\b", re.I),
    "IN": re.compile(r"\b(india|indian)\b", re.I),
    "AU": re.compile(r"\b(australia|australian)\b", re.I),
    "JP": re.compile(r"\b(japan|japanese)\b", re.I),
}

_METRIC_PATTERNS = {
    "cpc": re.compile(r"\bcost.per.click\b|\bcpc\b", re.I),
    "cpa": re.compile(r"\bcost.per.(applicant|application)\b|\bcpa\b", re.I),
    "cph": re.compile(r"\bcost.per.hire\b|\bcph\b", re.I),
    "apply_rate": re.compile(r"\bapply.rate\b|\bapplication.rate\b", re.I),
    "time_to_fill": re.compile(r"\btime.to.fill\b", re.I),
}

_VERTICAL_KEYWORDS = {
    "healthcare_nursing": ["nurse", "nursing", " rn ", "lpn", "cna"],
    "healthcare_general": ["healthcare", "medical", "clinician", "physician"],
    "trucking": ["truck", "cdl", "driver"],
    "gig": ["gig", "delivery", "rideshare"],
    "warehouse": ["warehouse", "forklift", "picker"],
    "retail": ["retail", "cashier", "store associate"],
    "tech": ["software", "developer", "engineer", "data scien"],
}


def _extract_metadata(text: str, source_file: str, key_path: str) -> dict[str, Any]:
    """Best-effort metadata extraction from chunk text + key path.

    Args:
        text: The chunk text.
        source_file: Source JSON filename (e.g. "joveo_cpa_benchmarks_2026.json").
        key_path: Dot-separated key path within the JSON.

    Returns:
        Metadata dict. Missing fields are simply absent (Qdrant filters
        treat absent fields as "does not match", which is correct).
    """
    md: dict[str, Any] = {
        "source_file": source_file,
        "kb_section": key_path,
    }
    haystack = f"{key_path} {text[:1000]}".lower()

    # Year: prefer the year mentioned in the filename, then in text.
    year_match = re.search(r"(20\d{2})", source_file)
    if year_match:
        md["year"] = int(year_match.group(1))
    else:
        year_match = re.search(r"\b(20\d{2})\b", haystack)
        if year_match:
            md["year"] = int(year_match.group(1))

    # Country.
    for code, pat in _COUNTRY_PATTERNS.items():
        if pat.search(haystack):
            md["country"] = code
            break

    # Metric.
    for metric, pat in _METRIC_PATTERNS.items():
        if pat.search(haystack):
            md["metric"] = metric
            break

    # Vertical.
    for vertical, kws in _VERTICAL_KEYWORDS.items():
        if any(kw in haystack for kw in kws):
            md["vertical"] = vertical
            break

    return md


def _approx_tokens(text: str) -> int:
    """Rough token count. 1 token ~= 4 chars for English."""
    return max(1, len(text) // 4)


def _chunk_json(data: Any, source_file: str, prefix: str = "") -> list[tuple[str, str]]:
    """Recursively chunk a JSON value into (key_path, text) tuples.

    Strategy:
        - On a dict with all-scalar children: emit one chunk
          ``"key1: v1\\nkey2: v2\\n..."``.
        - On a dict with nested dicts/lists: recurse, append non-scalar
          children separately. Also emit a summary chunk of scalar
          children at this level if any exist.
        - On a list: recurse into each item with an indexed key path.
        - On a scalar string >= 20 chars: emit as a chunk.

    Args:
        data: JSON value (dict, list, or scalar).
        source_file: Source filename for the chunk prefix.
        prefix: Dot-separated key path so far.

    Returns:
        List of (key_path, text) tuples.
    """
    chunks: list[tuple[str, str]] = []

    if isinstance(data, dict):
        scalars: list[str] = []
        for key, value in data.items():
            if key.startswith("_") or key in ("metadata", "last_updated", "version"):
                continue
            sub_prefix = f"{prefix}.{key}" if prefix else key
            if isinstance(value, (dict, list)):
                chunks.extend(_chunk_json(value, source_file, sub_prefix))
            elif isinstance(value, str) and len(value) >= 20:
                scalars.append(f"{key}: {value}")
            elif value is not None and not isinstance(value, str):
                scalars.append(f"{key}: {value}")
        if scalars:
            text = f"[{source_file}] {prefix}\n" + "\n".join(scalars)
            chunks.append((prefix, text))

    elif isinstance(data, list):
        for i, item in enumerate(data):
            sub_prefix = f"{prefix}[{i}]"
            chunks.extend(_chunk_json(item, source_file, sub_prefix))

    elif isinstance(data, str) and len(data) >= 20:
        chunks.append((prefix, f"[{source_file}] {prefix}: {data}"))

    return chunks


def _enforce_token_window(chunks: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Split overlong chunks and merge tiny ones into the 80-800 token window."""
    out: list[tuple[str, str]] = []
    pending_key: str | None = None
    pending_text: list[str] = []
    pending_tokens = 0

    def flush() -> None:
        nonlocal pending_key, pending_text, pending_tokens
        if pending_text:
            out.append((pending_key or "", "\n".join(pending_text)))
        pending_key = None
        pending_text = []
        pending_tokens = 0

    for key_path, text in chunks:
        n = _approx_tokens(text)
        if n > _CHUNK_MAX_TOKENS:
            # Split a giant chunk on newlines.
            flush()
            lines = text.split("\n")
            buf: list[str] = []
            buf_tokens = 0
            for line in lines:
                lt = _approx_tokens(line)
                if buf_tokens + lt > _CHUNK_MAX_TOKENS and buf:
                    out.append((key_path, "\n".join(buf)))
                    # Overlap: carry the last 80-token tail.
                    tail: list[str] = []
                    tail_tokens = 0
                    for ln in reversed(buf):
                        tail.append(ln)
                        tail_tokens += _approx_tokens(ln)
                        if tail_tokens >= _CHUNK_OVERLAP_TOKENS:
                            break
                    buf = list(reversed(tail))
                    buf_tokens = sum(_approx_tokens(ln) for ln in buf)
                buf.append(line)
                buf_tokens += lt
            if buf:
                out.append((key_path, "\n".join(buf)))
        elif n < _CHUNK_MIN_TOKENS:
            # Accumulate small chunks until we reach the minimum window.
            if pending_key is None:
                pending_key = key_path
            pending_text.append(text)
            pending_tokens += n
            if pending_tokens >= _CHUNK_MIN_TOKENS:
                flush()
        else:
            flush()
            out.append((key_path, text))
    flush()
    return out


# ---------------------------------------------------------------------------
# NovaRAGPipeline: orchestrator
# ---------------------------------------------------------------------------


class NovaRAGPipeline:
    """Retrieval-Augmented Generation engine for Nova (Phase 1 promotion).

    Public surface (per RAG_Design_2026.md):
        - ``embed_documents(docs)``
        - ``index(documents)``
        - ``retrieve(query, top_k=5, filters=None)``
        - ``rerank(query, hits, top_k=5)``
        - ``format_for_llm(hits)``

    Args:
        embedder: Embedding backend; auto-selected if None.
        store: Vector store; auto-selected if None.
        bm25: BM25 keyword index; created internally if None.
        reranker: Cross-encoder reranker; created internally if None.
        rerank_enabled: If False, skip rerank step.
    """

    def __init__(
        self,
        embedder: _EmbeddingBackend | None = None,
        store: _VectorStore | None = None,
        bm25: BM25Index | None = None,
        reranker: _Reranker | None = None,
        rerank_enabled: bool = True,
    ) -> None:
        self.embedder = embedder or _make_embedder()
        self.store = store or _make_store(dim=self.embedder.dim)
        self.bm25 = bm25 or BM25Index()
        self.reranker = reranker or _Reranker(enabled=rerank_enabled)
        # Mirror of indexed docs for BM25 lookup. In production this lives
        # in Qdrant payloads; we keep an in-process copy because BM25 needs
        # access to text and metadata at search time.
        self._docs_by_id: dict[str, Document] = {}

    # ----- Indexing --------------------------------------------------------

    def embed_documents(self, docs: list[Document]) -> list[list[float]]:
        """Embed a batch of documents.

        Args:
            docs: Documents to embed.

        Returns:
            List of embedding vectors, parallel to docs.
        """
        texts = [d.text for d in docs]
        vectors: list[list[float]] = []
        # Batch in chunks to respect Voyage's 128/call limit.
        for i in range(0, len(texts), 64):
            batch = texts[i : i + 64]
            vectors.extend(self.embedder.embed_batch(batch))
        return vectors

    def index(self, documents: list[Document]) -> dict[str, int]:
        """Embed and upsert documents into vector + BM25 indices.

        Args:
            documents: Documents to index.

        Returns:
            Dict with ``indexed`` (count upserted) and ``bm25`` (count in
            BM25 index after this call).
        """
        if not documents:
            return {"indexed": 0, "bm25": 0}
        logger.info("Indexing %d documents...", len(documents))
        t0 = time.monotonic()

        vectors = self.embed_documents(documents)
        n = self.store.upsert(documents, vectors)

        for doc in documents:
            self._docs_by_id[doc.doc_id] = doc
        self.bm25.index(list(self._docs_by_id.values()))

        elapsed = time.monotonic() - t0
        logger.info(
            "Indexed %d docs in %.1fs (%.1f docs/s).",
            n,
            elapsed,
            n / elapsed if elapsed > 0 else 0.0,
        )
        return {"indexed": n, "bm25": len(self._docs_by_id)}

    # ----- Retrieval -------------------------------------------------------

    def retrieve(
        self,
        query: str,
        top_k: int = _DEFAULT_TOP_K,
        filters: dict[str, Any] | None = None,
    ) -> list[RetrievalHit]:
        """Hybrid retrieval: vector + BM25 -> RRF -> rerank.

        Args:
            query: Natural-language query.
            top_k: Number of hits to return (default 5).
            filters: Optional metadata pre-filter for the vector leg.
                E.g. ``{"country": "US", "metric": "cpa", "year": 2026}``.

        Returns:
            Ranked list of RetrievalHit. Empty list if no candidates.
        """
        if not query or not query.strip():
            return []

        # Per-query cache of hydrated docs so candidate materialization is
        # free when the store returns payloads inline (production hot path,
        # where ``_docs_by_id`` is empty because the corpus lives in Qdrant).
        hydrated: dict[str, Document] = {}

        # Vector leg.
        vector_hits: list[tuple[str, float]] = []
        try:
            q_vec = (
                self.embedder.embed_query(query)
                if hasattr(self.embedder, "embed_query")
                else self.embedder.embed_batch([query])[0]
            )
            payload_hits = self.store.search_payload(q_vec, _VECTOR_FETCH_K, filters)
            if payload_hits is not None:
                # One round trip returned scores + documents together.
                for doc_id, score, doc in payload_hits:
                    vector_hits.append((doc_id, score))
                    hydrated[doc_id] = doc
            else:
                vector_hits = self.store.search(q_vec, _VECTOR_FETCH_K, filters)
        except Exception as exc:  # noqa: BLE001 -- degrade to BM25-only
            logger.warning("Vector retrieval failed: %s", exc, exc_info=True)

        # BM25 leg. BM25 doesn't natively support filters; we apply them
        # post-hoc against our in-process doc mirror.
        bm25_hits_raw = self.bm25.search(query, _BM25_FETCH_K)
        bm25_hits: list[tuple[str, float]] = []
        for doc_id, score in bm25_hits_raw:
            doc = self._docs_by_id.get(doc_id)
            if doc is None:
                continue
            if filters and not InMemoryStore._passes_filter(doc, filters):
                continue
            bm25_hits.append((doc_id, score))

        # Fuse with reciprocal rank fusion.
        fused = self._rrf(vector_hits, bm25_hits, k=_RRF_K)

        # Drop below-noise-floor candidates.
        fused = [(d, s) for d, s in fused if s >= _RRF_SCORE_FLOOR]

        # Materialize candidates. Prefer the inline-hydrated payloads, then
        # the in-process mirror, then a (slow) per-doc store fetch.
        candidates: list[RetrievalHit] = []
        for doc_id, score in fused[:_RERANK_INPUT_K]:
            doc = (
                hydrated.get(doc_id)
                or self._docs_by_id.get(doc_id)
                or self.store.get(doc_id)
            )
            if doc is None:
                continue
            candidates.append(
                RetrievalHit(
                    doc_id=doc.doc_id,
                    text=doc.text,
                    metadata=doc.metadata,
                    score=score,
                    source_file=doc.metadata.get("source_file", "unknown"),
                    search_method="hybrid_rrf",
                )
            )

        # Cross-encoder rerank to top_k.
        return self.rerank(query, candidates, top_k)

    @staticmethod
    def _rrf(
        a: list[tuple[str, float]],
        b: list[tuple[str, float]],
        k: int = _RRF_K,
    ) -> list[tuple[str, float]]:
        """Reciprocal Rank Fusion. Cormack/Clarke/Buettcher 2009."""
        scores: dict[str, float] = {}
        for rank, (doc_id, _) in enumerate(a):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
        for rank, (doc_id, _) in enumerate(b):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [(d, round(s, 6)) for d, s in ranked]

    def rerank(
        self, query: str, hits: list[RetrievalHit], top_k: int
    ) -> list[RetrievalHit]:
        """Cross-encoder rerank. No-op if rerank unavailable."""
        return self.reranker.rerank(query, hits, top_k)

    # ----- LLM formatting --------------------------------------------------

    @staticmethod
    def format_for_llm(hits: list[RetrievalHit]) -> str:
        """Format hits as an LLM-injectable evidence block.

        Output schema (consumed by the existing nova.py system_prompt
        synthesis):

            <kb_evidence>
              <chunk id="..." source="file.json" section="benchmarks.cpa.nursing"
                     score="0.78" search="rerank">
                text content
              </chunk>
              ...
            </kb_evidence>

        XML-style because Claude is known to handle XML-delimited context
        cleanly (Anthropic prompt engineering docs). The LLM is instructed
        to cite chunks by ``source``.
        """
        if not hits:
            return ""
        lines = ["<kb_evidence>"]
        for h in hits:
            attrs = (
                f'id="{h.doc_id[:8]}" '
                f'source="{h.source_file}" '
                f'section="{h.metadata.get("kb_section", "")}" '
                f'score="{h.score:.3f}" '
                f'search="{h.search_method}"'
            )
            lines.append(f"  <chunk {attrs}>")
            for line in h.text.splitlines():
                lines.append(f"    {line}")
            lines.append("  </chunk>")
        lines.append("</kb_evidence>")
        return "\n".join(lines)


# Backward-compatible alias. Older code (incl. the sketch's own pytests)
# imports ``NovaRAG``; keep the symbol so they continue to work.
NovaRAG = NovaRAGPipeline


# ---------------------------------------------------------------------------
# Indexing helper: walk the data/ directory and produce Documents
# ---------------------------------------------------------------------------


# Mirrors vector_search.index_knowledge_base() at module load time. In
# production this list comes from a config file so the index is
# reproducible across deploys.
_KB_FILES = (
    "recruitment_industry_knowledge.json",
    "joveo_2026_benchmarks.json",
    "joveo_cpa_benchmarks_2026.json",
    "recruitment_benchmarks_comprehensive_2026.json",
    "international_benchmarks_2026.json",
    "channels_db.json",
    "joveo_publishers.json",
    "platform_intelligence_deep.json",
    "salary_benchmarks_detailed_2026.json",
    "ad_benchmarks_recruitment_2026.json",
    "client_media_plans_kb.json",
    "nova_learned_answers.json",
    # 2026-06-12: Joveo client media-plan deck (methodology, push/pull, CPA
    # reference, sample pricing, why-Joveo, case study). Mirrors the entry in
    # vector_search.index_knowledge_base() and kb_loader.KB_FILES.
    "joveo_media_plan_deck_2026.json",
)


def build_documents_from_kb(
    data_dir: Path, files: Iterable[str] = _KB_FILES
) -> list[Document]:
    """Walk data/*.json and produce indexable Documents.

    Args:
        data_dir: Path to the data/ directory.
        files: Subset of filenames to index.

    Returns:
        List of Documents ready for ``NovaRAGPipeline.index``.
    """
    docs: list[Document] = []
    for filename in files:
        fpath = data_dir / filename
        if not fpath.exists():
            logger.info("Skipping missing KB file: %s", filename)
            continue
        try:
            with fpath.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load %s: %s", filename, exc)
            continue

        raw_chunks = _chunk_json(payload, source_file=filename)
        normalized = _enforce_token_window(raw_chunks)
        for i, (key_path, text) in enumerate(normalized):
            md = _extract_metadata(text=text, source_file=filename, key_path=key_path)
            md["chunk_index"] = i
            md["token_count"] = _approx_tokens(text)
            md["indexed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            # Deterministic UUID5 keeps re-indexing idempotent.
            ns = uuid.UUID("12345678-1234-5678-1234-567812345678")
            doc_id = str(uuid.uuid5(ns, f"{filename}::{key_path}::{i}"))
            docs.append(Document(doc_id=doc_id, text=text, metadata=md))
    return docs


__all__ = [
    "NovaRAGPipeline",
    "NovaRAG",
    "Document",
    "RetrievalHit",
    "BM25Index",
    "build_documents_from_kb",
    # REST backends (primary, no-SDK path).
    "VoyageRESTEmbedder",
    "QdrantRESTStore",
]
