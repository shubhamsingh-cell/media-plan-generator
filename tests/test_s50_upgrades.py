#!/usr/bin/env python3
"""S50 model and reranker upgrade verification tests.

Covers four production code changes (May 2026):
    1. llm_router.py    -- GPT4O -> gpt-5.4-mini, CLAUDE -> sonnet-4-6,
                           CLAUDE_OPUS -> opus-4-7, OPENROUTER -> qwen3-coder
                           (each with env-override fallback)
    2. vector_search.py -- new _rerank_with_voyage() using rerank-2.5-lite,
                           with graceful fallback to keyword overlap
    3. edge_router.py   -- display label updates (xAI Grok 4.3, Sonnet 4.6,
                           Opus 4.7); display-only, no API behavior change
    4. routes/health.py -- display label updates (Qwen3 Coder, Grok 4.3)

Test tiers (run all by default; live tier auto-skips when keys absent):
    Tier 1: STATIC VALIDATION   -- imports, dict shape, constants
    Tier 2: SYNTAX + STYLE      -- ast parse, no stale model strings
    Tier 3: RERANKER UNIT       -- mocked urlopen, success + every fallback path
    Tier 4: LIVE API SMOKE      -- 1-token ping per provider (gated by env)
    Tier 5: NOVA FLOW SMOKE     -- existing chat / RAG / health still work

No production code is modified.

Run:
    cd media-plan-generator
    python3 -m pytest tests/test_s50_upgrades.py -v

Run only static (no network) tiers:
    python3 -m pytest tests/test_s50_upgrades.py -v -m "not live"
"""

from __future__ import annotations

import ast
import importlib
import io
import json
import os
import re
import sys
import textwrap
import urllib.error
from pathlib import Path
from typing import Any, Dict
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

LLM_ROUTER_PATH = PROJECT_ROOT / "llm_router.py"
VECTOR_SEARCH_PATH = PROJECT_ROOT / "vector_search.py"
EDGE_ROUTER_PATH = PROJECT_ROOT / "edge_router.py"
HEALTH_PATH = PROJECT_ROOT / "routes" / "health.py"

PRODUCTION_PY_FILES = sorted(
    list(PROJECT_ROOT.glob("*.py")) + list((PROJECT_ROOT / "routes").glob("*.py"))
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _strip_comments_and_strings_from_ast(source: str) -> ast.Module:
    """Parse source so we can inspect string literals separately from comments."""
    return ast.parse(source)


def _string_literals(tree: ast.AST) -> list[tuple[int, str]]:
    """Return every (line, value) string literal in the AST."""
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            out.append((node.lineno, node.value))
    return out


def _comments_in(source: str) -> list[tuple[int, str]]:
    """Return every (line, text) Python comment in source."""
    out: list[tuple[int, str]] = []
    for i, line in enumerate(source.splitlines(), start=1):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            out.append((i, stripped))
        elif "#" in line:
            # Best-effort inline-comment detector (skips '#' inside string lits).
            in_s: str | None = None
            for j, ch in enumerate(line):
                if in_s is None and ch in ("'", '"'):
                    in_s = ch
                elif in_s and ch == in_s and (j == 0 or line[j - 1] != "\\"):
                    in_s = None
                elif in_s is None and ch == "#":
                    out.append((i, line[j:].strip()))
                    break
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# Tier 1: STATIC VALIDATION  (no network)
# ═══════════════════════════════════════════════════════════════════════════════


class TestTier1StaticValidation:
    """Verify modules import and expose the expected post-S50 surface."""

    def test_llm_router_imports(self) -> None:
        """llm_router must import without raising."""
        import llm_router  # noqa: F401

    def test_vector_search_imports(self) -> None:
        """vector_search must import without raising."""
        import vector_search  # noqa: F401

    def test_edge_router_imports(self) -> None:
        """edge_router must import without raising."""
        import edge_router  # noqa: F401

    def test_routes_health_imports(self) -> None:
        """routes.health must import without raising."""
        import routes.health  # noqa: F401

    def test_subprocess_import_check_exits_zero(self) -> None:
        """The exact command from the task spec must exit 0."""
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; sys.path.insert(0, '"
                + str(PROJECT_ROOT)
                + "'); import llm_router; import vector_search; import edge_router",
            ],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"Import command failed (rc={result.returncode}): "
            f"stderr={result.stderr.decode()[:500]}"
        )

    # ── PROVIDER_CONFIG dict shape ──────────────────────────────────────────

    def test_provider_config_has_expected_keys(self) -> None:
        """The four upgraded provider IDs must all live in PROVIDER_CONFIG."""
        from llm_router import (
            CLAUDE,
            CLAUDE_OPUS,
            GPT4O,
            OPENROUTER,
            PROVIDER_CONFIG,
        )

        for pid in (CLAUDE, CLAUDE_OPUS, GPT4O, OPENROUTER):
            assert pid in PROVIDER_CONFIG, f"Missing provider id: {pid}"
            cfg = PROVIDER_CONFIG[pid]
            assert isinstance(cfg, dict), f"{pid} config is not a dict"
            for field in ("model", "name", "endpoint", "env_key", "api_style"):
                assert field in cfg, f"{pid} missing field {field!r}"
            assert (
                isinstance(cfg["model"], str) and cfg["model"]
            ), f"{pid} has empty model"

    def test_gpt4o_uses_gpt_5_4_mini(self) -> None:
        """GPT4O entry must point to gpt-5.4-mini by default."""
        from llm_router import GPT4O, PROVIDER_CONFIG

        # Re-evaluate without OPENAI_MODEL override leaking from CI env.
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OPENAI_MODEL", None)
            importlib.reload(importlib.import_module("llm_router"))
            from llm_router import PROVIDER_CONFIG as fresh_cfg

            assert fresh_cfg[GPT4O]["model"] == "gpt-5.4-mini"

    def test_claude_sonnet_uses_4_6(self) -> None:
        """CLAUDE entry must point to claude-sonnet-4-6 by default."""
        from llm_router import CLAUDE

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CLAUDE_SONNET_MODEL", None)
            importlib.reload(importlib.import_module("llm_router"))
            from llm_router import PROVIDER_CONFIG as fresh_cfg

            assert fresh_cfg[CLAUDE]["model"] == "claude-sonnet-4-6"

    def test_claude_opus_uses_4_7(self) -> None:
        """CLAUDE_OPUS entry must point to claude-opus-4-7 by default."""
        from llm_router import CLAUDE_OPUS

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CLAUDE_OPUS_MODEL", None)
            importlib.reload(importlib.import_module("llm_router"))
            from llm_router import PROVIDER_CONFIG as fresh_cfg

            assert fresh_cfg[CLAUDE_OPUS]["model"] == "claude-opus-4-7"

    def test_openrouter_uses_qwen3_coder(self) -> None:
        """OPENROUTER entry must point to qwen/qwen3-coder:free by default."""
        from llm_router import OPENROUTER

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OPENROUTER_MODEL", None)
            importlib.reload(importlib.import_module("llm_router"))
            from llm_router import PROVIDER_CONFIG as fresh_cfg

            assert fresh_cfg[OPENROUTER]["model"] == "qwen/qwen3-coder:free"

    def test_env_overrides_take_effect(self) -> None:
        """Setting the four env vars overrides the default model strings."""
        with mock.patch.dict(
            os.environ,
            {
                "OPENAI_MODEL": "gpt-4o",
                "CLAUDE_SONNET_MODEL": "claude-sonnet-4-20250514",
                "CLAUDE_OPUS_MODEL": "claude-opus-4-20250514",
                "OPENROUTER_MODEL": "meta-llama/llama-4-maverick:free",
            },
            clear=False,
        ):
            importlib.reload(importlib.import_module("llm_router"))
            from llm_router import (
                CLAUDE,
                CLAUDE_OPUS,
                GPT4O,
                OPENROUTER,
                PROVIDER_CONFIG,
            )

            assert PROVIDER_CONFIG[GPT4O]["model"] == "gpt-4o"
            assert PROVIDER_CONFIG[CLAUDE]["model"] == "claude-sonnet-4-20250514"
            assert PROVIDER_CONFIG[CLAUDE_OPUS]["model"] == "claude-opus-4-20250514"
            assert (
                PROVIDER_CONFIG[OPENROUTER]["model"]
                == "meta-llama/llama-4-maverick:free"
            )

        # Reload one more time so subsequent tests see the canonical defaults.
        for k in (
            "OPENAI_MODEL",
            "CLAUDE_SONNET_MODEL",
            "CLAUDE_OPUS_MODEL",
            "OPENROUTER_MODEL",
        ):
            os.environ.pop(k, None)
        importlib.reload(importlib.import_module("llm_router"))

    # ── vector_search constants ─────────────────────────────────────────────

    def test_voyage_rerank_constants_exist(self) -> None:
        """vector_search must expose the new rerank constants with right values."""
        import vector_search

        assert hasattr(vector_search, "_VOYAGE_RERANK_URL")
        assert hasattr(vector_search, "_VOYAGE_RERANK_MODEL")
        assert hasattr(vector_search, "_VOYAGE_RERANK_TIMEOUT")
        assert vector_search._VOYAGE_RERANK_URL == "https://api.voyageai.com/v1/rerank"
        assert vector_search._VOYAGE_RERANK_MODEL == "rerank-2.5-lite"
        assert isinstance(vector_search._VOYAGE_RERANK_TIMEOUT, (int, float))
        assert vector_search._VOYAGE_RERANK_TIMEOUT > 0

    def test_voyage_rerank_function_exists(self) -> None:
        """_rerank_with_voyage and _rerank_results must be callable."""
        import vector_search

        assert callable(getattr(vector_search, "_rerank_with_voyage", None))
        assert callable(getattr(vector_search, "_rerank_results", None))

    def test_voyage_4_migration_note_present(self) -> None:
        """A migration note about voyage-3 -> voyage-4 must exist as a comment."""
        src = _read(VECTOR_SEARCH_PATH)
        # Be lenient about formatting -- look for the key tokens.
        assert (
            "voyage-4" in src.lower()
        ), "Expected a comment mentioning voyage-4 migration"
        assert (
            "reindex" in src.lower() or "migrat" in src.lower()
        ), "Expected a note about reindex/migration in vector_search"

    # ── edge_router display labels ──────────────────────────────────────────

    def test_edge_router_grok_label(self) -> None:
        """Edge router must show 'xAI Grok 4.3' display label."""
        from edge_router import PROVIDER_CATALOG

        assert "xai" in PROVIDER_CATALOG
        assert PROVIDER_CATALOG["xai"]["name"] == "xAI Grok 4.3"

    def test_edge_router_claude_sonnet_label(self) -> None:
        """Edge router must show 'Claude Sonnet 4.6' display label."""
        from edge_router import PROVIDER_CATALOG

        assert "claude" in PROVIDER_CATALOG
        assert PROVIDER_CATALOG["claude"]["name"] == "Claude Sonnet 4.6"

    def test_edge_router_claude_opus_label(self) -> None:
        """Edge router must show 'Claude Opus 4.7' display label."""
        from edge_router import PROVIDER_CATALOG

        assert "claude_opus" in PROVIDER_CATALOG
        assert PROVIDER_CATALOG["claude_opus"]["name"] == "Claude Opus 4.7"

    # ── routes/health display labels ────────────────────────────────────────

    def test_health_qwen3_coder_label(self) -> None:
        """routes/health.py must list 'Qwen3 Coder 480B (free)' label."""
        src = _read(HEALTH_PATH)
        assert (
            "Qwen3 Coder 480B (free)" in src
        ), "Expected 'Qwen3 Coder 480B (free)' display label"
        assert (
            "Llama 4 Maverick" in src
        ), "Expected deprecation note mentioning Llama 4 Maverick to remain"

    def test_health_grok_4_3_label(self) -> None:
        """routes/health.py must list 'Grok 4.3' label with new pricing note."""
        src = _read(HEALTH_PATH)
        assert "Grok 4.3" in src, "Expected 'Grok 4.3' display label"
        # New pricing note signals this is post-S50 wording.
        assert (
            "1.25" in src and "2.50" in src
        ), "Expected new $1.25 / $2.50 pricing note for Grok 4.3"

    # ── llm_router docstring ─────────────────────────────────────────────────

    def test_llm_router_docstring_mentions_gemini_3_flash(self) -> None:
        """Module docstring must reference Gemini 3 Flash, not 2.0."""
        import llm_router

        doc = llm_router.__doc__ or ""
        assert (
            "Gemini 3 Flash" in doc
        ), "Module docstring should mention 'Gemini 3 Flash'"
        assert (
            "Gemini 2.0 Flash" not in doc
        ), "Stale 'Gemini 2.0 Flash' reference must not remain in docstring"


# ═══════════════════════════════════════════════════════════════════════════════
# Tier 2: SYNTAX + STYLE  (no network)
# ═══════════════════════════════════════════════════════════════════════════════


class TestTier2SyntaxStyle:
    """AST-level checks: parsable, no stale active model strings."""

    @pytest.mark.parametrize(
        "path",
        [LLM_ROUTER_PATH, VECTOR_SEARCH_PATH, EDGE_ROUTER_PATH, HEALTH_PATH],
        ids=lambda p: p.name,
    )
    def test_modified_files_parse(self, path: Path) -> None:
        """Each modified file must be valid Python (compile + ast.parse)."""
        src = _read(path)
        # Both routes confirm syntactic validity.
        compile(src, str(path), "exec")
        tree = ast.parse(src)
        assert isinstance(tree, ast.Module)

    def test_no_old_claude_strings_as_active_literals_in_modified_files(
        self,
    ) -> None:
        """Stale Claude model strings must not survive as literals in S50 files.

        The four files modified in S50 should reference the old strings only
        in env-override fallback comments, NOT as `ast.Constant` values.
        Other production files are flagged separately in the dedicated test.
        """
        forbidden = {"claude-sonnet-4-20250514", "claude-opus-4-20250514"}
        offenders: list[str] = []

        for path in (
            LLM_ROUTER_PATH,
            VECTOR_SEARCH_PATH,
            EDGE_ROUTER_PATH,
            HEALTH_PATH,
        ):
            src = _read(path)
            tree = ast.parse(src)
            for line, value in _string_literals(tree):
                if value in forbidden:
                    offenders.append(f"{path.name}:{line}: literal {value!r}")

        assert not offenders, (
            "Old Claude model strings still appear as active string "
            "literals (not just comments):\n  " + "\n  ".join(offenders)
        )

    def test_old_models_in_other_files_documented(self) -> None:
        """Surface any stale model strings outside the S50-modified files.

        This test is informational -- it does NOT fail the build but emits
        a clear xfail-style message so reviewers can decide whether the
        legacy callsites in app.py / nova.py need a follow-up upgrade.
        """
        forbidden = {
            "claude-sonnet-4-20250514",
            "claude-opus-4-20250514",
            "meta-llama/llama-4-maverick",
        }
        modified = {LLM_ROUTER_PATH, VECTOR_SEARCH_PATH, EDGE_ROUTER_PATH, HEALTH_PATH}
        findings: list[str] = []

        for path in PRODUCTION_PY_FILES:
            if path in modified:
                continue
            try:
                src = _read(path)
                tree = ast.parse(src)
            except (SyntaxError, UnicodeDecodeError):
                continue
            for line, value in _string_literals(tree):
                for needle in forbidden:
                    if needle in value:
                        findings.append(
                            f"{path.relative_to(PROJECT_ROOT)}:{line}: {value!r}"
                        )

        if findings:
            # Documented finding -- xfail rather than fail so the build is
            # green but the report is loud.
            pytest.xfail(
                "Legacy model strings remain in non-S50 files (consider "
                "upgrading in a follow-up):\n  " + "\n  ".join(findings)
            )

    def test_no_active_gpt_4o_literal_in_llm_router(self) -> None:
        """`gpt-4o` may live in comments / env fallback notes, not as an active model."""
        src = _read(LLM_ROUTER_PATH)
        tree = ast.parse(src)

        # The default model is set with: os.environ.get("OPENAI_MODEL") or "gpt-5.4-mini"
        # i.e. the right-hand "or" string MUST be the new model. Any standalone
        # 'gpt-4o' constant in the GPT4O config block would be stale.
        offenders: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict) and any(
                isinstance(k, ast.Constant) and k.value == "model" for k in node.keys
            ):
                # Only inspect the value paired with the "model" key.
                for k, v in zip(node.keys, node.values):
                    if (
                        isinstance(k, ast.Constant)
                        and k.value == "model"
                        and isinstance(v, ast.Constant)
                        and isinstance(v.value, str)
                        and v.value == "gpt-4o"
                    ):
                        offenders.append(f"line {v.lineno}: model='gpt-4o' constant")

        assert (
            not offenders
        ), "gpt-4o still appears as an active 'model' value: " + "; ".join(offenders)

    def test_no_active_maverick_in_openrouter_model_field(self) -> None:
        """Active OPENROUTER 'model' field must not be Llama 4 Maverick."""
        from llm_router import OPENROUTER, PROVIDER_CONFIG

        active = PROVIDER_CONFIG[OPENROUTER]["model"]
        assert (
            "meta-llama/llama-4-maverick" not in active
        ), f"OPENROUTER.model is still Llama 4 Maverick: {active!r}"


# ═══════════════════════════════════════════════════════════════════════════════
# Tier 3: RERANKER UNIT TESTS  (mocked network)
# ═══════════════════════════════════════════════════════════════════════════════


class _FakeUrlopenContext:
    """Minimal context-manager that mimics urllib.request.urlopen()."""

    def __init__(self, payload: bytes) -> None:
        self._buf = io.BytesIO(payload)

    def __enter__(self) -> _FakeUrlopenContext:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def read(self) -> bytes:
        return self._buf.read()


class TestTier3VoyageRerankerUnit:
    """Unit tests for _rerank_with_voyage and _rerank_results.

    Network calls are mocked at urllib.request.urlopen. _get_api_key is
    patched so the codepath does not exit early when VOYAGE_API_KEY is unset
    in the test environment.
    """

    SAMPLE_RESULTS = [
        {"text": "linkedin cpc benchmarks 2026", "score": 0.5},
        {"text": "indeed performance metrics in healthcare", "score": 0.5},
        {"text": "general unrelated content about cars", "score": 0.5},
    ]

    QUERY = "linkedin cpc"

    def _voyage_payload(self, ranking: list[tuple[int, float]]) -> bytes:
        """Build a fake Voyage rerank response for given index->score pairs."""
        return json.dumps(
            {
                "data": [
                    {"index": idx, "relevance_score": score} for idx, score in ranking
                ]
            }
        ).encode("utf-8")

    # ── Success path ────────────────────────────────────────────────────────

    def test_voyage_success_reorders_by_score(self) -> None:
        """A successful Voyage call should reorder results per relevance_score."""
        import vector_search

        # Voyage returns idx 1 highest, then 0, then 2.
        payload = self._voyage_payload([(1, 0.95), (0, 0.80), (2, 0.10)])

        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch(
            "urllib.request.urlopen",
            return_value=_FakeUrlopenContext(payload),
        ):
            out = vector_search._rerank_with_voyage(
                list(self.SAMPLE_RESULTS), self.QUERY, top_k=3
            )

        assert out is not None, "Voyage success path returned None"
        assert len(out) == 3
        # Order must follow the mocked relevance order (idx 1, 0, 2).
        assert out[0]["text"] == self.SAMPLE_RESULTS[1]["text"]
        assert out[1]["text"] == self.SAMPLE_RESULTS[0]["text"]
        assert out[2]["text"] == self.SAMPLE_RESULTS[2]["text"]
        for r in out:
            assert r["rerank_method"] == "voyage_rerank_2_5_lite"
            assert "rerank_score" in r

    def test_voyage_success_top_k_truncates(self) -> None:
        """top_k must clip the returned list."""
        import vector_search

        payload = self._voyage_payload([(1, 0.95), (0, 0.80), (2, 0.10)])
        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch(
            "urllib.request.urlopen",
            return_value=_FakeUrlopenContext(payload),
        ):
            out = vector_search._rerank_with_voyage(
                list(self.SAMPLE_RESULTS), self.QUERY, top_k=2
            )

        assert out is not None
        assert len(out) == 2
        assert out[0]["text"] == self.SAMPLE_RESULTS[1]["text"]

    # ── Fallback paths ──────────────────────────────────────────────────────

    def test_voyage_returns_none_when_api_key_missing(self) -> None:
        """No api key -> return None so the orchestrator falls back."""
        import vector_search

        with mock.patch.object(vector_search, "_get_api_key", return_value=None):
            out = vector_search._rerank_with_voyage(
                list(self.SAMPLE_RESULTS), self.QUERY
            )

        assert out is None

    def test_voyage_returns_none_on_url_error(self) -> None:
        """A urllib URLError must yield None, not raise."""
        import vector_search

        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            out = vector_search._rerank_with_voyage(
                list(self.SAMPLE_RESULTS), self.QUERY
            )

        assert out is None

    def test_voyage_returns_none_on_http_error(self) -> None:
        """HTTPError (e.g. 500) must also yield None."""
        import vector_search

        err = urllib.error.HTTPError(
            url="https://api.voyageai.com/v1/rerank",
            code=500,
            msg="Server error",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )
        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch("urllib.request.urlopen", side_effect=err):
            out = vector_search._rerank_with_voyage(
                list(self.SAMPLE_RESULTS), self.QUERY
            )

        assert out is None

    def test_voyage_returns_none_on_timeout(self) -> None:
        """TimeoutError must yield None."""
        import vector_search

        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
            out = vector_search._rerank_with_voyage(
                list(self.SAMPLE_RESULTS), self.QUERY
            )

        assert out is None

    def test_voyage_returns_none_on_malformed_json(self) -> None:
        """Garbage response body must yield None, not raise."""
        import vector_search

        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch(
            "urllib.request.urlopen",
            return_value=_FakeUrlopenContext(b"not json {{{"),
        ):
            out = vector_search._rerank_with_voyage(
                list(self.SAMPLE_RESULTS), self.QUERY
            )

        assert out is None

    def test_voyage_returns_none_on_empty_data(self) -> None:
        """API returning {data: []} should yield None so orchestrator falls back."""
        import vector_search

        payload = json.dumps({"data": []}).encode("utf-8")
        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch(
            "urllib.request.urlopen",
            return_value=_FakeUrlopenContext(payload),
        ):
            out = vector_search._rerank_with_voyage(
                list(self.SAMPLE_RESULTS), self.QUERY
            )

        assert out is None

    def test_voyage_handles_alternate_results_key(self) -> None:
        """Some Voyage versions return 'results' instead of 'data'."""
        import vector_search

        payload = json.dumps(
            {"results": [{"index": 0, "relevance_score": 0.9}]}
        ).encode("utf-8")
        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch(
            "urllib.request.urlopen",
            return_value=_FakeUrlopenContext(payload),
        ):
            out = vector_search._rerank_with_voyage(
                list(self.SAMPLE_RESULTS), self.QUERY, top_k=3
            )

        assert out is not None
        assert len(out) == 1
        assert out[0]["rerank_method"] == "voyage_rerank_2_5_lite"

    def test_voyage_passes_through_empty_inputs(self) -> None:
        """Empty results or query must short-circuit (mirror upstream behavior)."""
        import vector_search

        # Empty results: the function returns the input unchanged.
        out = vector_search._rerank_with_voyage([], self.QUERY)
        assert out == []
        # Empty query: same.
        out = vector_search._rerank_with_voyage(list(self.SAMPLE_RESULTS), "")
        assert out == self.SAMPLE_RESULTS

    # ── Orchestrator: _rerank_results ──────────────────────────────────────

    def test_rerank_results_uses_voyage_when_available(self) -> None:
        """_rerank_results should prefer Voyage on success."""
        import vector_search

        payload = self._voyage_payload([(1, 0.99), (0, 0.5), (2, 0.1)])
        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch(
            "urllib.request.urlopen",
            return_value=_FakeUrlopenContext(payload),
        ):
            out = vector_search._rerank_results(
                list(self.SAMPLE_RESULTS), self.QUERY, top_k=3
            )

        assert len(out) == 3
        # First item is the one Voyage scored highest (idx 1).
        assert out[0]["text"] == self.SAMPLE_RESULTS[1]["text"]
        assert out[0]["rerank_method"] == "voyage_rerank_2_5_lite"

    def test_rerank_results_falls_back_to_keyword_overlap(self) -> None:
        """When Voyage fails, _rerank_results uses keyword overlap scoring."""
        import vector_search

        with mock.patch.object(
            vector_search, "_get_api_key", return_value="fake-key"
        ), mock.patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("network"),
        ):
            out = vector_search._rerank_results(
                list(self.SAMPLE_RESULTS), self.QUERY, top_k=3
            )

        # Fallback path stamps a different rerank_method on each item.
        assert all(r.get("rerank_method") == "keyword_overlap_fallback" for r in out)
        # Items containing 'linkedin' or 'cpc' should rank above unrelated ones.
        assert "linkedin cpc" in out[0]["text"]
        # combined_score is set on the fallback path.
        assert all("combined_score" in r for r in out)

    def test_rerank_results_no_api_key_falls_back(self) -> None:
        """No VOYAGE_API_KEY -> keyword fallback is used."""
        import vector_search

        with mock.patch.object(vector_search, "_get_api_key", return_value=None):
            out = vector_search._rerank_results(
                list(self.SAMPLE_RESULTS), self.QUERY, top_k=3
            )

        assert all(r.get("rerank_method") == "keyword_overlap_fallback" for r in out)

    def test_rerank_results_empty_input(self) -> None:
        """Empty input must return empty list without raising."""
        import vector_search

        out = vector_search._rerank_results([], self.QUERY)
        assert out == []


# ═══════════════════════════════════════════════════════════════════════════════
# Tier 4: LIVE API SMOKE TESTS  (auto-skipped without keys)
# ═══════════════════════════════════════════════════════════════════════════════
#
# These are real HTTP calls. Each uses a 1-token "ping" to confirm the model
# string is accepted by the upstream provider. Tests are gated on env vars,
# so CI runs without keys remain green.


def _post_json(
    url: str, body: dict, headers: dict, timeout: int = 20
) -> tuple[int, dict | None, str]:
    """Tiny stdlib POST helper. Returns (status, json or None, raw text)."""
    import urllib.request
    import urllib.error

    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return resp.status, json.loads(raw), raw
            except json.JSONDecodeError:
                return resp.status, None, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if e.fp else ""
        try:
            return e.code, json.loads(raw), raw
        except json.JSONDecodeError:
            return e.code, None, raw
    except urllib.error.URLError as e:
        return 0, None, f"network error: {e}"
    except TimeoutError as e:
        return 0, None, f"timeout: {e}"


@pytest.mark.live
class TestTier4LiveAPI:
    """Live smoke tests -- skipped automatically when API keys are missing."""

    @staticmethod
    def _skip_if(env_key: str) -> str:
        api_key = os.environ.get(env_key)
        if not api_key:
            pytest.skip(f"{env_key} not set; skipping live test")
        return api_key

    @staticmethod
    def _model_rejected(payload: dict | None, raw: str) -> bool:
        """Detect 'model not found' style errors in API responses."""
        text = (raw or "").lower()
        if any(
            phrase in text
            for phrase in (
                "model not found",
                "does not exist",
                "invalid model",
                "unknown model",
                "no such model",
                "model_not_found",
                "deprecated",
            )
        ):
            return True
        if isinstance(payload, dict):
            err = payload.get("error", payload)
            if isinstance(err, dict):
                code = (err.get("code") or err.get("type") or "").lower()
                msg = (err.get("message") or "").lower()
                if "model" in code and ("not_found" in code or "invalid" in code):
                    return True
                if "model" in msg and "not found" in msg:
                    return True
        return False

    # ── Anthropic ──────────────────────────────────────────────────────────

    def _anthropic_ping(self, model: str) -> None:
        api_key = self._skip_if("ANTHROPIC_API_KEY")
        status, payload, raw = _post_json(
            "https://api.anthropic.com/v1/messages",
            body={
                "model": model,
                "max_tokens": 1,
                "messages": [{"role": "user", "content": "ping"}],
            },
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            timeout=20,
        )
        if self._model_rejected(payload, raw):
            pytest.fail(
                f"Anthropic rejected model {model!r}: status={status} body={raw[:300]}"
            )
        assert (
            status == 200
        ), f"Anthropic {model} returned non-200: status={status} body={raw[:300]}"

    def test_anthropic_haiku_live(self) -> None:
        """Claude Haiku 4.5 (unchanged baseline) must still accept ping."""
        self._anthropic_ping("claude-haiku-4-5-20251001")

    def test_anthropic_sonnet_live(self) -> None:
        """Claude Sonnet 4.6 (S50 upgrade) must accept ping."""
        from llm_router import CLAUDE, PROVIDER_CONFIG

        self._anthropic_ping(PROVIDER_CONFIG[CLAUDE]["model"])

    def test_anthropic_opus_live(self) -> None:
        """Claude Opus 4.7 (S50 upgrade) must accept ping."""
        from llm_router import CLAUDE_OPUS, PROVIDER_CONFIG

        self._anthropic_ping(PROVIDER_CONFIG[CLAUDE_OPUS]["model"])

    # ── OpenAI ──────────────────────────────────────────────────────────────

    def test_openai_gpt54mini_live(self) -> None:
        """gpt-5.4-mini (S50 upgrade) must be a valid OpenAI model string."""
        api_key = self._skip_if("OPENAI_API_KEY")
        from llm_router import GPT4O, PROVIDER_CONFIG

        model = PROVIDER_CONFIG[GPT4O]["model"]

        status, payload, raw = _post_json(
            "https://api.openai.com/v1/chat/completions",
            body={
                "model": model,
                "messages": [{"role": "user", "content": "ping"}],
                "max_tokens": 1,
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            timeout=20,
        )
        if self._model_rejected(payload, raw):
            pytest.fail(
                f"OpenAI rejected model {model!r}: status={status} body={raw[:300]}"
            )
        assert (
            status == 200
        ), f"OpenAI {model} returned non-200: status={status} body={raw[:300]}"

    # ── Gemini ──────────────────────────────────────────────────────────────

    def test_gemini_3_flash_live(self) -> None:
        """Gemini 3 Flash (or whatever GEMINI is configured for) must accept ping."""
        api_key = self._skip_if("GEMINI_API_KEY")
        from llm_router import GEMINI, PROVIDER_CONFIG

        cfg = PROVIDER_CONFIG[GEMINI]
        model = cfg["model"]
        url = (
            "https://generativelanguage.googleapis.com/v1beta/"
            f"models/{model}:generateContent?key={api_key}"
        )
        status, payload, raw = _post_json(
            url,
            body={
                "contents": [{"parts": [{"text": "ping"}]}],
                "generationConfig": {"maxOutputTokens": 1},
            },
            headers={"Content-Type": "application/json"},
            timeout=20,
        )
        if self._model_rejected(payload, raw):
            pytest.fail(
                f"Gemini rejected model {model!r}: status={status} body={raw[:300]}"
            )
        assert (
            status == 200
        ), f"Gemini {model} returned non-200: status={status} body={raw[:300]}"

    # ── OpenRouter Qwen3 Coder ─────────────────────────────────────────────

    def test_openrouter_qwen3coder_live(self) -> None:
        """qwen/qwen3-coder:free (S50 upgrade) must accept ping via OpenRouter."""
        api_key = self._skip_if("OPENROUTER_API_KEY")
        from llm_router import OPENROUTER, PROVIDER_CONFIG

        cfg = PROVIDER_CONFIG[OPENROUTER]
        status, payload, raw = _post_json(
            cfg["endpoint"],
            body={
                "model": cfg["model"],
                "messages": [{"role": "user", "content": "ping"}],
                "max_tokens": 1,
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                **cfg.get("extra_headers", {}),
            },
            timeout=25,
        )
        if self._model_rejected(payload, raw):
            pytest.fail(
                f"OpenRouter rejected model {cfg['model']!r}: "
                f"status={status} body={raw[:300]}"
            )
        # OpenRouter free tier sometimes returns 429 on burst -- allow that
        # but treat 4xx other than 429 as failure.
        if status == 429:
            pytest.skip("OpenRouter free tier rate-limited; not a model issue")
        assert (
            status == 200
        ), f"OpenRouter {cfg['model']} non-200: status={status} body={raw[:300]}"

    # ── Voyage embeddings + rerank ─────────────────────────────────────────

    def test_voyage_embed_live(self) -> None:
        """Voyage embeddings must produce a vector of expected dimension."""
        api_key = self._skip_if("VOYAGE_API_KEY")
        import vector_search

        status, payload, raw = _post_json(
            vector_search._VOYAGE_API_URL,
            body={"input": "test", "model": vector_search._VOYAGE_MODEL},
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            timeout=20,
        )
        assert status == 200, f"Voyage embed non-200: {status} body={raw[:300]}"
        assert payload is not None
        data = payload.get("data") or []
        assert data, f"Voyage embed payload empty: {raw[:200]}"
        embedding = data[0].get("embedding") or []
        # voyage-3-lite -> 512 dims (per _QDRANT_VECTOR_DIM = 512).
        assert len(embedding) == vector_search._QDRANT_VECTOR_DIM, (
            f"Embedding dim mismatch: got {len(embedding)} "
            f"expected {vector_search._QDRANT_VECTOR_DIM}"
        )

    def test_voyage_rerank_live(self) -> None:
        """Voyage rerank API must return relevance-ordered indices for 3 docs."""
        api_key = self._skip_if("VOYAGE_API_KEY")
        import vector_search

        status, payload, raw = _post_json(
            vector_search._VOYAGE_RERANK_URL,
            body={
                "query": "linkedin cpc",
                "documents": [
                    "linkedin advertising cpc benchmarks",
                    "indeed performance metrics",
                    "general tax law overview",
                ],
                "model": vector_search._VOYAGE_RERANK_MODEL,
                "top_k": 3,
                "truncation": True,
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            timeout=20,
        )
        assert status == 200, f"Voyage rerank non-200: status={status} body={raw[:300]}"
        assert payload is not None
        entries = payload.get("data") or payload.get("results") or []
        assert len(entries) >= 1, f"No rerank entries: {raw[:200]}"
        for e in entries:
            assert "index" in e
            assert "relevance_score" in e or "score" in e, f"Missing score field in {e}"


# ═══════════════════════════════════════════════════════════════════════════════
# Tier 5: NOVA FLOW SMOKE TESTS  (auto-skipped without TEST_BASE_URL)
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.live
class TestTier5NovaFlowSmoke:
    """Don't break what works -- existing chat / RAG / health endpoints.

    Skipped unless TEST_BASE_URL is set (defaults are too risky to hit prod
    accidentally from local dev). Set TEST_BASE_URL=http://localhost:10000
    after a local server is running.
    """

    BASE_URL_ENV = "TEST_BASE_URL"

    @classmethod
    def _base(cls) -> str:
        url = os.environ.get(cls.BASE_URL_ENV)
        if not url:
            pytest.skip(
                f"{cls.BASE_URL_ENV} not set; skipping Nova smoke (set to "
                f"http://localhost:10000 to enable)"
            )
        return url.rstrip("/")

    def _get(self, path: str, timeout: int = 15) -> tuple[int, str]:
        import urllib.request
        import urllib.error

        url = f"{self._base()}{path}"
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return resp.status, resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", errors="replace") if e.fp else ""
        except (urllib.error.URLError, TimeoutError) as e:
            pytest.skip(f"Server unreachable at {url}: {e}")

    def test_existing_health_check(self) -> None:
        """/api/health/ping must return 200 (cheapest health probe)."""
        status, _ = self._get("/api/health/ping")
        assert status == 200, f"Health ping returned {status}"

    def test_existing_chat_flow(self) -> None:
        """Existing chat endpoint must still respond to a trivial prompt."""
        import urllib.request
        import urllib.error

        body = json.dumps({"message": "ping", "conversation_id": "s50-smoke"}).encode(
            "utf-8"
        )
        req = urllib.request.Request(
            f"{self._base()}/api/chat",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                status = resp.status
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            status = e.code
            raw = e.read().decode("utf-8", errors="replace") if e.fp else ""
        except (urllib.error.URLError, TimeoutError) as e:
            pytest.skip(f"Chat endpoint unreachable: {e}")
            return

        # 200 is ideal; 401/403 indicates auth wall (still proves the route is
        # alive after the upgrade, which is what we want from this smoke test).
        assert status in (
            200,
            401,
            403,
        ), f"Chat returned unexpected status {status}: {raw[:300]}"

    def test_existing_kb_search(self) -> None:
        """Vector search RAG retrieval must still work end-to-end."""
        try:
            import vector_search
        except Exception as e:
            pytest.skip(f"vector_search import failed: {e}")
            return
        # vector_search.search() handles graceful empty-index responses,
        # so this is a true end-to-end probe of the retrieval path.
        results = vector_search.search("recruitment marketing benchmarks", top_k=3)
        assert isinstance(results, list)
        # Empty index is acceptable in fresh test env; we only assert no crash.


# ═══════════════════════════════════════════════════════════════════════════════
# Tier 6: GEMINI THINKING-BUDGET CONTROL (post-S50 verification fix)
# ═══════════════════════════════════════════════════════════════════════════════
#
# gemini-3-flash-preview / gemini-3.1-flash-lite-preview are thinking-enabled
# by default. Without thinkingConfig.thinkingBudget = 0, requests with small
# maxOutputTokens spend the budget on internal thinking and time out or
# return empty. These tests verify the centralized auto-rule and overrides
# in llm_router._build_gemini_request and _stream_gemini.


class TestTier6GeminiThinkingBudget:
    """Verify thinkingBudget is injected per the documented decision rule.

    The auto rule (from _should_disable_gemini_thinking):
        - Per-call disable_thinking=True/False wins.
        - GEMINI_DISABLE_THINKING env var is the next override.
        - Otherwise: tools -> keep thinking; max_tokens < 2048 -> disable;
          max_tokens >= 2048 -> keep.
    """

    @staticmethod
    def _payload_for(
        max_tokens: int,
        tools: Any = None,
        disable_thinking: Any = None,
    ) -> Dict[str, Any]:
        """Helper -- build a request and return the parsed JSON payload.

        Sets a placeholder GEMINI_API_KEY so the request URL builds cleanly.
        """
        # Reload so env-var changes take effect for this call.
        import llm_router as r

        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"}, clear=False):
            kwargs: Dict[str, Any] = {
                "messages": [{"role": "user", "content": "hi"}],
                "system_prompt": "",
                "max_tokens": max_tokens,
                "tools": tools,
            }
            if disable_thinking is not None:
                kwargs["disable_thinking"] = disable_thinking
            _, _, body_bytes = r._build_gemini_request(**kwargs)
            return json.loads(body_bytes.decode("utf-8"))

    def test_auto_disables_thinking_for_small_max_tokens(self) -> None:
        """maxOutputTokens < 2048 with no tools -> thinkingBudget=0 must be present."""
        # Wipe env override so the auto rule applies.
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GEMINI_DISABLE_THINKING", None)
            payload = self._payload_for(max_tokens=512, tools=None)
        gen_cfg = payload["generationConfig"]
        assert gen_cfg["maxOutputTokens"] == 512
        assert gen_cfg.get("thinkingConfig", {}).get("thinkingBudget") == 0, (
            "Small maxOutputTokens with no tools must disable thinking "
            "(otherwise gemini-3-flash spends the budget thinking and "
            "returns empty). Got: " + json.dumps(gen_cfg)
        )

    def test_auto_keeps_thinking_for_large_max_tokens(self) -> None:
        """maxOutputTokens >= 2048 with no tools -> no thinkingConfig set (auto-on)."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GEMINI_DISABLE_THINKING", None)
            payload = self._payload_for(max_tokens=4096, tools=None)
        gen_cfg = payload["generationConfig"]
        assert gen_cfg["maxOutputTokens"] == 4096
        assert "thinkingConfig" not in gen_cfg, (
            "Large maxOutputTokens (long-form synthesis) should keep thinking "
            "enabled by default. Got: " + json.dumps(gen_cfg)
        )

    def test_auto_keeps_thinking_when_tools_present(self) -> None:
        """Tools always keep thinking on, even with small maxOutputTokens."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GEMINI_DISABLE_THINKING", None)
            payload = self._payload_for(
                max_tokens=512,
                tools=[
                    {
                        "name": "lookup",
                        "description": "x",
                        "input_schema": {"type": "object", "properties": {}},
                    }
                ],
            )
        gen_cfg = payload["generationConfig"]
        assert "thinkingConfig" not in gen_cfg, (
            "Function-calling benefits from thinking; auto rule must keep it. "
            "Got: " + json.dumps(gen_cfg)
        )
        # Also confirm the tool definition still flows through.
        assert "tools" in payload, "tool definitions should still be present"

    def test_per_call_override_true_forces_disable(self) -> None:
        """disable_thinking=True must inject thinkingBudget=0 even with large tokens."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GEMINI_DISABLE_THINKING", None)
            payload = self._payload_for(
                max_tokens=8192, tools=None, disable_thinking=True
            )
        gen_cfg = payload["generationConfig"]
        assert gen_cfg.get("thinkingConfig", {}).get("thinkingBudget") == 0

    def test_per_call_override_false_keeps_thinking(self) -> None:
        """disable_thinking=False must keep thinking on even when auto would disable."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GEMINI_DISABLE_THINKING", None)
            payload = self._payload_for(
                max_tokens=128, tools=None, disable_thinking=False
            )
        gen_cfg = payload["generationConfig"]
        assert "thinkingConfig" not in gen_cfg

    def test_env_var_force_disable(self) -> None:
        """GEMINI_DISABLE_THINKING=1 must disable thinking globally."""
        with mock.patch.dict(os.environ, {"GEMINI_DISABLE_THINKING": "1"}, clear=False):
            # Even with large max_tokens AND tools, env override wins.
            payload = self._payload_for(
                max_tokens=8192,
                tools=[
                    {
                        "name": "lookup",
                        "description": "x",
                        "input_schema": {"type": "object", "properties": {}},
                    }
                ],
            )
        gen_cfg = payload["generationConfig"]
        assert gen_cfg.get("thinkingConfig", {}).get("thinkingBudget") == 0, (
            "GEMINI_DISABLE_THINKING=1 should force-disable thinking. Got: "
            + json.dumps(gen_cfg)
        )

    def test_env_var_force_keep_suppresses_auto(self) -> None:
        """GEMINI_DISABLE_THINKING=0 must suppress the auto-disable rule."""
        with mock.patch.dict(os.environ, {"GEMINI_DISABLE_THINKING": "0"}, clear=False):
            # Small max_tokens would normally trigger the auto-disable.
            payload = self._payload_for(max_tokens=128, tools=None)
        gen_cfg = payload["generationConfig"]
        assert (
            "thinkingConfig" not in gen_cfg
        ), "GEMINI_DISABLE_THINKING=0 must suppress auto-disable. Got: " + json.dumps(
            gen_cfg
        )

    def test_per_call_override_beats_env_var(self) -> None:
        """Per-call disable_thinking has the highest precedence."""
        with mock.patch.dict(os.environ, {"GEMINI_DISABLE_THINKING": "0"}, clear=False):
            payload = self._payload_for(
                max_tokens=128, tools=None, disable_thinking=True
            )
        gen_cfg = payload["generationConfig"]
        assert gen_cfg.get("thinkingConfig", {}).get("thinkingBudget") == 0


# NOTE: the 'live' pytest marker used above is registered in tests/conftest.py
# so that `pytest -m "not live"` skips live-network tests with no warnings.
