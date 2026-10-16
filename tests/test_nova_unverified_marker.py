"""Numbers the model invented must reach the reader MARKED.

Pre-fix (P1.3) nova stripped the ``_[unverified]_`` tag before display, so a
figure that traced to no tool result looked exactly like one from the planning
engine. Now each such figure carries a dagger and the reply ends with one
footer line.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

for _name in (
    "anthropic",
    "openai",
    "supabase",
    "redis",
    "qdrant_client",
    "sentence_transformers",
    "posthog",
):
    sys.modules.setdefault(_name, types.ModuleType(_name))

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import nova  # noqa: E402

FOOTER = "† estimate not traced to Nova's planning engine"
CANNED = "Nurse CPA in London is around 145 GBP and the apply rate is 14.0% on Indeed UK."
MESSAGE = "tell me about nurse recruiting costs in london"


@pytest.fixture
def engine(monkeypatch):
    monkeypatch.setenv("NOVA_NUMBER_VERIFIER_ENABLED", "1")
    monkeypatch.setenv("NOVA_CITATIONS_ENABLED", "1")
    return nova.Nova.__new__(nova.Nova)


def _result(text=CANNED):
    return {"response": text, "sources": [], "confidence": 0.8, "tools_used": []}


def test_unverified_figure_gets_a_dagger_and_one_footer(engine):
    out = engine._apply_t3_postprocessing(_result(), MESSAGE)
    text = out["response"]
    assert "14.0%†" in text
    assert text.count(FOOTER) == 1
    assert "_[unverified]_" not in text
    assert out["unverified_numbers"] >= 1


def test_second_pass_does_not_duplicate_the_footer(engine):
    once = engine._apply_t3_postprocessing(_result(), MESSAGE)
    twice = engine._apply_t3_postprocessing(once, MESSAGE)
    assert twice["response"].count(FOOTER) == 1


def test_a_reply_with_no_unverified_number_has_no_footer(engine):
    out = engine._apply_t3_postprocessing(_result("Indeed is a large job board."), MESSAGE)
    assert "†" not in out["response"]


def test_dagger_stays_outside_bold_spans(engine):
    out = engine._apply_t3_postprocessing(
        _result("The apply rate is **14.0%** on Indeed UK."), MESSAGE
    )
    assert "**14.0%**†" in out["response"]
