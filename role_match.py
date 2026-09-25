"""Shared role-title phrase matching.

Role lookup tables (H-1B aliases, the gold-standard difficulty map) used
to match a title with a raw substring test or on ANY single shared word.
That let blue-collar plant titles borrow white-collar profiles: "ee"
inside "engineer", "cto" inside "director", and "Machine Operator"
sharing the word "machine" with "machine learning engineer" (H-1B
Data Scientist wage; Senior 9/10 Executive Search difficulty) on a
Hershey plant plan (2026-09-24).

``match_role_phrase`` is the one rule both lookups use: a phrase matches
only as whole words, or when EVERY word of the phrase is in the title --
never on a single shared word.
"""

from __future__ import annotations

import re
from typing import Iterable, Optional


def _words(text: str) -> set[str]:
    return {w for w in re.split(r"\W+", text) if w}


def match_role_phrase(title_lower: str, phrases: Iterable[str]) -> Optional[str]:
    """Return the best phrase from *phrases* that matches *title_lower*.

    1. Whole-phrase match (word boundaries on both sides); the longest
       matching phrase wins, so "senior software engineer" beats
       "software engineer".
    2. Word-bag match: every word of the phrase appears in the title, in
       any order (e.g. "Electrical Controls Engineer" -> "electrical
       engineer"); the phrase with the most words wins.

    Returns None when neither rule matches. *title_lower* must already be
    lower-cased; phrases are expected lower-case.
    """
    title = (title_lower or "").strip()
    if not title:
        return None
    ordered = sorted(phrases, key=len, reverse=True)
    for phrase in ordered:
        if phrase and re.search(rf"\b{re.escape(phrase)}\b", title):
            return phrase
    title_words = _words(title)
    best: Optional[str] = None
    best_score = 0
    for phrase in ordered:
        phrase_words = _words(phrase)
        if phrase_words and phrase_words <= title_words and len(phrase_words) > best_score:
            best_score = len(phrase_words)
            best = phrase
    return best
