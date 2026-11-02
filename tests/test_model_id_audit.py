"""Model-id audit: every hard-coded Claude / DeepSeek / Gemini id is current.

Scans the CODE lines (comments skipped -- they legitimately name retired ids
as history and rollback notes) of the four modules that pin model ids, so a
stale id reintroduced anywhere fails here instead of 404-ing in production.

Current ids (2026-11-03 audit):
  * Claude: the 5.5 family only.
  * DeepSeek: deepseek-v4-flash / deepseek-v4-pro (official API catalog), also
    as OpenRouter's "deepseek/<id>" slugs; "deepseek/deepseek-r1" left
    OpenRouter's public model catalog and "deepseek-v4-pro-max" never existed.
  * Gemini: exactly the ids the router pins for its two Gemini providers.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import List, Tuple

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
    assert cost["input"] > 0 and cost["output"] > 0  # paid route, never free


def test_official_deepseek_provider_pins() -> None:
    cfg = lr.PROVIDER_CONFIG[lr.DEEPSEEK]
    assert {cfg["model"], cfg["model_fast"]} == DEEPSEEK_IDS
