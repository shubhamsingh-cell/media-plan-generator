"""Model-id audit: every hard-coded Claude / DeepSeek / Gemini id is current.

Scans the CODE lines (comments skipped -- they legitimately name retired ids
as history and rollback notes) of the four modules that pin model ids, so a
stale id reintroduced anywhere fails here instead of 404-ing in production.

Current ids (2026-11-03 audit):
  * Claude: the 5.5 family only.
  * DeepSeek: deepseek-v4-flash / deepseek-v4-pro (official API catalog), also
    as OpenRouter's "deepseek/<id>" slugs. The bare "deepseek/deepseek-r1"
    slug is not in OpenRouter's public catalog (the dated r1-0528 still is, but
    the repo pins DeepSeek to V4); "deepseek-v4-pro-max" never existed.
  * Gemini: exactly the ids the router pins for its two Gemini providers.
  * OpenRouter free routes: the ":free" slugs below are listed in OpenRouter's
    public catalog (GET /api/v1/models, 2026-11-03) with pricing.prompt ==
    pricing.completion == "0"; the six earlier free slugs were delisted.
  * Groq / Cerebras: the Llama 4 Scout (Groq) and llama-3.3-70b (Cerebras)
    ids are deprecated per each provider's deprecation page; both routes
    serve gpt-oss-120b, the documented replacement.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import List, Tuple

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import llm_router as lr  # noqa: E402

FILES = ("llm_router.py", "nova.py", "app.py", "multi_agent.py")
CLAUDE_IDS = {"claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5"}
DEEPSEEK_IDS = {"deepseek-v4-flash", "deepseek-v4-pro"}
_ID_RE = re.compile(r"[\"'](claude-[a-z0-9.-]+|deepseek[a-z0-9./:-]*|gemini-[a-z0-9.-]+)[\"']")


def _code_ids() -> List[Tuple[str, int, str]]:
    out: List[Tuple[str, int, str]] = []
    for name in FILES:
        for lineno, line in enumerate((PROJECT_ROOT / name).read_text("utf-8").splitlines(), 1):
            code = line.split("#", 1)[0]  # model ids never contain '#'
            for match in _ID_RE.finditer(code):
                out.append((name, lineno, match.group(1)))
    return out


def test_scanner_sees_the_pinned_ids() -> None:
    ids = {i for _, _, i in _code_ids()}
    assert {"claude-sonnet-5-5", "deepseek-v4-pro", "gemini-3.6-flash"} <= ids


def test_claude_ids_are_the_5_5_family() -> None:
    bad = [(f, n, i) for f, n, i in _code_ids() if i.startswith("claude-") and i not in CLAUDE_IDS]
    assert bad == []


def test_deepseek_ids_are_v4_flash_or_pro() -> None:
    bad = []
    for f, n, i in _code_ids():
        if not i.startswith("deepseek") or i == "deepseek":  # bare provider key
            continue
        model = i.split("/", 1)[1] if i.startswith("deepseek/") else i
        if model not in DEEPSEEK_IDS:
            bad.append((f, n, i))
    assert bad == []


def test_gemini_ids_match_the_router_pins() -> None:
    pins = {lr.PROVIDER_CONFIG[lr.GEMINI]["model"], lr.PROVIDER_CONFIG[lr.GEMINI_FLASH_LITE]["model"]}
    assert pins == {"gemini-3.6-flash", "gemini-3.5-flash-lite"}
    bad = [(f, n, i) for f, n, i in _code_ids() if i.startswith("gemini-") and i not in pins]
    assert bad == []


def test_openrouter_deepseek_route_is_a_current_slug_and_priced() -> None:
    cfg = lr.PROVIDER_CONFIG[lr.OPENROUTER_DEEPSEEK_R1]
    assert cfg["model"] in {f"deepseek/{m}" for m in DEEPSEEK_IDS}
    cost = lr._PROVIDER_COST_PER_M_TOKENS[lr.OPENROUTER_DEEPSEEK_R1]
    assert cost == lr._openrouter_deepseek_cost(cfg["model"])


def test_openrouter_deepseek_prices_are_the_audited_catalog_values() -> None:
    # USD per 1M tokens, GET https://openrouter.ai/api/v1/models, 2026-11-03.
    assert lr._OPENROUTER_DEEPSEEK_PRICE_PER_M == {
        "deepseek/deepseek-v4-flash": {"input": 0.091, "output": 0.182},
        "deepseek/deepseek-v4-pro": {"input": 1.60, "output": 3.20},
    }


def test_cost_follows_the_model_override_and_never_understates() -> None:
    assert lr._openrouter_deepseek_cost("deepseek/deepseek-v4-pro") == {"input": 1.60, "output": 3.20}
    assert lr._openrouter_deepseek_cost("deepseek/deepseek-v4-flash") == {"input": 0.091, "output": 0.182}
    # An unknown override is booked at the highest known price.
    assert lr._openrouter_deepseek_cost("deepseek/some-future-model") == {"input": 1.60, "output": 3.20}


def test_cost_table_is_built_from_the_configured_slug() -> None:
    src = (PROJECT_ROOT / "llm_router.py").read_text("utf-8")
    assert re.search(
        r"OPENROUTER_DEEPSEEK_R1:\s*_openrouter_deepseek_cost\(\s*"
        r"PROVIDER_CONFIG\[OPENROUTER_DEEPSEEK_R1\]\[\"model\"\]",
        src,
    )


def test_official_deepseek_provider_pins() -> None:
    cfg = lr.PROVIDER_CONFIG[lr.DEEPSEEK]
    assert {cfg["model"], cfg["model_fast"]} == DEEPSEEK_IDS


# --- OpenRouter free routes + Groq/Cerebras (2026-11-03 audit) --------------

# route constant name -> (exact pinned model id, env var that overrides it or "")
EXPECTED_OPENROUTER_FREE = {
    "OPENROUTER": ("nvidia/nemotron-3-ultra-550b-a55b:free", "OPENROUTER_MODEL"),
    "OPENROUTER_QWEN": ("cohere/north-mini-code:free", ""),
    "OPENROUTER_ARCEE": ("nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free", ""),
    "OPENROUTER_LIQUID": ("liquid/lfm-2.5-2.6b:free", ""),
    "OPENROUTER_YI": ("xingchen-agi/xing4.0-29b-a4b:free", ""),
    "OPENROUTER_GEMMA": ("google/gemma-4-31b-it:free", ""),
    "OPENROUTER_GPT_OSS": ("nvidia/nemotron-3.5-super-vl:free", "OPENROUTER_GPT_OSS_MODEL"),
}

# Slugs absent from OpenRouter's public catalog (or, for Groq/Cerebras, listed
# as deprecated/removed by the provider) -- must never come back as code.
DEAD_SLUGS = (
    "qwen/qwen3-coder:free",
    "google/gemma-3-27b-it:free",
    "openai/gpt-oss-120b:free",
    "arcee-ai/arcee-trinity-v1:free",
    "liquid/lfm-2.5:free",
    "01-ai/yi-large:free",
    "meta-llama/llama-4-scout-17b-16e-instruct",  # Groq, shut down 07/17/26
)


def _router_code_lines() -> List[Tuple[int, str]]:
    src = (PROJECT_ROOT / "llm_router.py").read_text("utf-8").splitlines()
    return [(n, line.split("#", 1)[0]) for n, line in enumerate(src, 1)]


def _pin(route_const: str) -> str:
    """Pinned model id for a route; skip when an env override is active."""
    expected, env_var = EXPECTED_OPENROUTER_FREE.get(route_const, ("", ""))
    if env_var and os.environ.get(env_var):
        pytest.skip(f"{env_var} override is set in this environment")
    return lr.PROVIDER_CONFIG[getattr(lr, route_const)]["model"]


@pytest.mark.parametrize("route_const", sorted(EXPECTED_OPENROUTER_FREE))
def test_openrouter_free_route_pins_listed_free_slug(route_const: str) -> None:
    expected, _ = EXPECTED_OPENROUTER_FREE[route_const]
    assert _pin(route_const) == expected
    assert expected.endswith(":free")


@pytest.mark.parametrize("route_const", sorted(EXPECTED_OPENROUTER_FREE))
def test_openrouter_free_route_is_costed_as_free(route_const: str) -> None:
    cost = lr._PROVIDER_COST_PER_M_TOKENS[getattr(lr, route_const)]
    assert cost == {"input": 0.0, "output": 0.0}


def test_no_delisted_slug_in_router_code() -> None:
    """Comments may cite retired slugs as history; code may not use them."""
    hits = [
        (n, slug)
        for n, code in _router_code_lines()
        for slug in DEAD_SLUGS
        if f'"{slug}"' in code or f"'{slug}'" in code
    ]
    assert hits == []


def test_groq_and_cerebras_use_gpt_oss_120b() -> None:
    if os.environ.get("GROQ_MODEL"):
        pytest.skip("GROQ_MODEL override is set in this environment")
    assert lr.PROVIDER_CONFIG[lr.GROQ]["model"] == "openai/gpt-oss-120b"
    assert lr.PROVIDER_CONFIG[lr.CEREBRAS]["model"] == "gpt-oss-120b"
    # the deprecated Cerebras id must not survive as a quoted code literal
    stale = [n for n, code in _router_code_lines() if re.search(r"[\"']llama-3\.3-70b[\"']", code)]
    assert stale == []
