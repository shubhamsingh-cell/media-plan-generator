"""Without a writing model, a "latest news" question is told plainly that
live news could not be fetched (sweep 2026-11-03: it got the generic
capabilities blurb as if the question had been answered)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nova  # noqa: E402


def test_news_question_degrades_honestly() -> None:
    nv = nova.Nova()
    res = nv._chat_rule_based(
        "What are the latest news this week about Indeed pricing changes?"
    )
    text = res["response"]
    assert "live" in text.lower() and ("news" in text.lower() or "current" in text.lower())
    assert "couldn't" in text.lower() or "could not" in text.lower() or "can't" in text.lower()


def test_ordinary_question_gets_no_news_disclaimer() -> None:
    res = nova.Nova()._chat_rule_based("cpc for nurses in Texas")
    assert "live news" not in res["response"].lower()
