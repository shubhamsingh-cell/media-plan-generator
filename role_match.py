"""Shared role-title phrase matching.

Role lookup tables (H-1B aliases, the gold-standard difficulty map and
salary ranges) used to match a title with a raw substring test or on ANY
single shared word. That let blue-collar plant titles borrow white-collar
profiles: "ee" inside "engineer", "cto" inside "director", and "Machine
Operator" sharing the word "machine" with "machine learning engineer" (H-1B
Data Scientist wage; Senior 9/10 Executive Search difficulty) on a Hershey
plant plan (2026-09-24); "cook" inside "cookie", "nurse" inside "nursery".

``match_role_phrase`` is the one rule every lookup uses: a phrase matches
only as whole words, or when EVERY word of the phrase is in the title --
never on a single shared word. Plurals ("Registered Nurses",
"Accountants") and developer titles ("Java Developer", "Frontend
Developer" = a software engineer) still match.
"""

from __future__ import annotations

import re
from typing import Iterable, Optional

# A "developer" title is a software engineer unless the title says it is
# another kind of developer ("Real Estate Developer", "Business Developer").
_NON_SOFTWARE_DEVELOPER = frozenset(
    {"business", "real", "estate", "property", "land", "community", "curriculum",
     "course", "talent", "leadership", "organizational", "organisational"}
)
_DEVELOPER_WORDS = frozenset({"developer", "developers", "dev", "devs"})


def _words(text: str) -> set[str]:
    return {w for w in re.split(r"\W+", text) if w}


def _singulars(word: str) -> set[str]:
    """The word plus its possible singular forms (-s / -es stripped)."""
    forms = {word}
    if len(word) > 3 and word.endswith("es"):
        forms.add(word[:-2])
    if len(word) > 2 and word.endswith("s") and not word.endswith("ss"):
        forms.add(word[:-1])
    return forms


def _title_word_forms(title: str) -> set[str]:
    """Every word of *title* plus singular forms, and -- for a software
    developer title -- the equivalent words "software" and "engineer"."""
    words = _words(title)
    forms: set[str] = set()
    for w in words:
        forms |= _singulars(w)
    if words & _DEVELOPER_WORDS and not words & _NON_SOFTWARE_DEVELOPER:
        forms |= {"developer", "software", "engineer"}
    return forms


def _phrase_regex(phrase: str) -> "re.Pattern[str]":
    """Whole-word pattern for *phrase*: each word may carry a plural -s/-es
    and words may be separated by spaces or hyphens."""
    parts = [re.escape(w) + r"(?:s|es)?" for w in re.split(r"[\s\-]+", phrase) if w]
    return re.compile(r"\b" + r"[\s\-]+".join(parts) + r"\b")


def match_role_phrase(title_lower: str, phrases: Iterable[str]) -> Optional[str]:
    """Return the best phrase from *phrases* that matches *title_lower*.

    1. Whole-phrase match (word boundaries on both sides, plural -s/-es and
       space/hyphen variants allowed); the longest matching phrase wins, so
       "senior software engineer" beats "software engineer".
    2. Word-bag match: every word of the phrase appears in the title (a
       plural title word counts as its singular; a software "developer"
       title also counts as "software engineer"), in any order -- e.g.
       "Electrical Controls Engineer" -> "electrical engineer", "Java
       Developer" -> "software developer"; the phrase with the most words
       wins.

    Returns None when neither rule matches. *title_lower* must already be
    lower-cased; phrases are expected lower-case.
    """
    title = (title_lower or "").strip()
    if not title:
        return None
    ordered = sorted(phrases, key=len, reverse=True)
    for phrase in ordered:
        if phrase and _phrase_regex(phrase).search(title):
            return phrase
    title_forms = _title_word_forms(title)
    best: Optional[str] = None
    best_score = 0
    for phrase in ordered:
        phrase_words = _words(phrase)
        if phrase_words and phrase_words <= title_forms and len(phrase_words) > best_score:
            best_score = len(phrase_words)
            best = phrase
    return best
