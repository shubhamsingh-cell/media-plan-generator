"""Nova's web_search tool says where its results came from.

With TAVILY_API_KEY unset, tavily_search.search silently falls back to Jina AI
/ DuckDuckGo, but Nova labelled every hit "source": "tavily"; and when every
tier failed the model got a bare "Web search unavailable" with nothing telling
it to say so instead of answering from memory as if current.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nova  # noqa: E402
import tavily_search  # noqa: E402

HIT = [{"title": "t", "url": "https://example.org/a", "content": "c", "score": 0.5}]


def _run(results, configured: bool, scraper=None):
    nv = nova.Nova.__new__(nova.Nova)
    with mock.patch.object(tavily_search, "_get_api_key", lambda: "k" if configured else None), \
        mock.patch.object(tavily_search, "search", lambda q, max_results=5: results), \
        mock.patch("web_scraper_router.search_web", lambda q: scraper):
        return nova.Nova._web_search(nv, {"query": "indeed pricing news this week"})


def test_fallback_hits_are_not_labelled_tavily() -> None:
    res = _run(HIT, configured=False)
    assert res["results"] == HIT
    assert res["source"] != "tavily"
    assert "Tavily" in res["provider_note"]


def test_tavily_hits_keep_their_label_when_configured() -> None:
    res = _run(HIT, configured=True)
    assert res["source"] == "tavily"


def test_no_results_tells_the_model_to_say_so() -> None:
    res = _run(None, configured=False, scraper=None)
    assert res["results"] == []
    note = res.get("note", "")
    assert "could not" in note.lower() and "current" in note.lower()
