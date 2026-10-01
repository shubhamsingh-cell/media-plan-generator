"""Competitor-claim rules shared by the deliverable renderers, the executive
narrative sanitizer (excel_v2) and the bundle_qa gate + repair pass.

Why this exists: the generation pipeline holds NO hiring evidence for any
competitor -- the wizard sends typed names only, and the web/ATS search the
wizard's "Analyze Careers Pages" button runs is never passed into
/api/generate (prod 2026-09-24: every competitor query failed anyway --
Tavily 432, Jina 401, DDG timeouts). Yet all four prod Hershey bundles
shipped a critical ``unsourced_competitor_claim``: the LLM executive
summary (Executive Summary!B58) wrote "...compounded by named competitors
(Nestle Purina, Campbell's, Land O'Lakes, Treehouse Foods) drawing from the
same..." -- model knowledge presented as fact.

Rules:
  * ``find_asserted_claims`` -- the asserted-third-party-behaviour verb
    phrases bundle_qa treats as critical, minus any sentence whose subject
    is the CLIENT itself ("where Hershey is drawing from the same ... talent"
    describes the plan's own client, not a competitor).
  * ``strip_claim_sentences`` -- drops the offending sentences (and, for a
    narrative, every sentence naming a no-evidence competitor).
  * ``competitor_has_evidence`` -- a competitor entry counts as evidenced
    only when it carries sourced content (a non-generic description, or an
    explicit evidence/source field); a bare typed name never does.
  * ``NO_EVIDENCE_LINE`` -- the neutral line a card shows instead of a claim.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import company_blurb

# Kept short on purpose: "Why: " + CLIENT_NAMED_LINE must stay ONE line in
# the slide-7 card's 5.7in usable width at 8pt (a ~104-char version sat on
# the wrap boundary and overprinted the Counter line in the geometry matrix).
NO_EVIDENCE_LINE = "No verified public hiring data available."
CLIENT_NAMED_LINE = f"Named in the brief as a competitor. {NO_EVIDENCE_LINE}"
INFERRED_LINE = f"Inferred from industry classification. {NO_EVIDENCE_LINE}"
# Group-level wording (design review 2026-10-01): when competitors have no
# evidence they are listed ONCE as a group with ONE neutral sentence --
# never a per-competitor card repeating the same line.
BRIEF_GROUP_LABEL = "Competitors named in your brief"
INFERRED_GROUP_LABEL = "Competitors inferred from industry classification"
GROUP_NO_EVIDENCE_SENTENCE = (
    "No verified public hiring data found for these employers; add evidence "
    "or run competitive intelligence to profile them."
)
# Workbook column headers for an employer list with no evidence record:
# never "Top Employers", which asserts a standing nobody observed.
NAMED_IN_BRIEF_HEADER = "Named in brief"
INFERRED_HEADER = "Inferred competitors"

# Generic capitalised sentence/label openers that precede the real name in
# "Why: <Name> ..." / "Counter: <Name> ..." text -- skipped by the name
# match so it keeps scanning to the actual company name.
NON_NAME_LEAD_WORDS: Tuple[str, ...] = (
    "Why",
    "Counter",
    "Where",
    "Expect",
    "Against",
    "This",
    "That",
    "These",
    "Those",
    "The",
    "A",
    "An",
    "To",
    "If",
    "Because",
    "Candidates",
    "Impact",
    "Mitigation",
)
_NON_NAME_LEAD_ALT = "|".join(re.escape(w) for w in NON_NAME_LEAD_WORDS)
# Up to 4 title-case words -- conservative on purpose (a false negative just
# misses a name; a false positive on a critical rule is the costlier error).
COMPETITOR_NAME_RE = (
    rf"(?!(?:{_NON_NAME_LEAD_ALT})\b)[A-Z][A-Za-z0-9&.,’'-]*"
    r"(?:\s+[A-Z][A-Za-z0-9&.,’'-]*){0,3}"
)
# Name -> verb-phrase bridge, capped at 60 non-period chars (one sentence).
CLAIM_BRIDGE_RE = r"[^.]{0,60}?"

ASSERTED_BEHAVIOR_PHRASES: Tuple[Tuple[str, str], ...] = (
    (
        r"actively\s+(?:recruiting|recruits|staffing|competing\s+for)\b",
        "actively recruits/staffs/competes for",
    ),
    (r"keeps?\s+pressure\s+on\b", "keeps pressure on"),
    (r"puts?\s+direct\s+pressure\s+on\b", "hiring activity puts direct pressure on"),
    (r"drawing\s+from\s+the\s+same\b", "is drawing from the same"),
    (r"is\s+slower\s+to\s+respond\b", "is slower to respond"),
    (r"is\s+hiring\b[^.]{0,60}?\bdirectly\b", "is hiring ... directly"),
    (r"has\s+been\s+especially\s+aggressive\b", "has been especially aggressive"),
)
ASSERTED_BEHAVIOR_VERB_RES: Tuple[Tuple["re.Pattern[str]", str], ...] = tuple(
    (re.compile(rf"({COMPETITOR_NAME_RE}){CLAIM_BRIDGE_RE}\b{phrase}"), label)
    for phrase, label in ASSERTED_BEHAVIOR_PHRASES
)

_GENERIC_DESCRIPTIONS = frozenset({"", "competitor", "industry competitor"})
# Deliberately NOT "source": pipeline entries use it for provenance labels
# ("brief", "industry_fallback") that are not evidence of anything.
_EVIDENCE_KEYS = ("evidence", "source_url", "sources")


def is_client_reference(name: Any, client_name: Any) -> bool:
    """True when a matched name is the plan's own client ("Hershey" for
    client "THE HERSHEY COMPANY")."""
    if not isinstance(name, str) or not isinstance(client_name, str):
        return False
    if not name.strip() or not client_name.strip():
        return False
    return company_blurb.name_agrees(client_name, name)


def find_asserted_claims(text: Any, client_name: Any = "") -> List[Dict[str, Any]]:
    """Every asserted-behaviour claim in ``text`` whose named subject is NOT
    the client. Each hit: ``{"name", "label", "start", "end"}``.

    A match whose name is the client does not end the scan of that span:
    the search resumes just past the client's name, so "Hershey and Mars are
    drawing from the same..." still reports Mars (verifier, 2026-10-01 --
    finditer resumed AFTER the whole match and never saw Mars)."""
    if not isinstance(text, str) or not text.strip():
        return []
    hits: List[Dict[str, Any]] = []
    for pattern, label in ASSERTED_BEHAVIOR_VERB_RES:
        pos = 0
        while pos < len(text):
            m = pattern.search(text, pos)
            if not m:
                break
            name = (m.group(1) or "").strip().rstrip(",")
            if name and not is_client_reference(name, client_name):
                hits.append(
                    {"name": name, "label": label, "start": m.start(), "end": m.end()}
                )
                pos = m.end()
            else:
                # step past the client's first word only, then keep looking
                first = re.match(r"\S+", text[m.start(1):])
                pos = m.start(1) + (len(first.group(0)) if first else 1)
    hits.sort(key=lambda h: h["start"])
    return hits


# Words that, right after a single-word name, mark it as a common noun or a
# metric label rather than the company ("Target CPA", "Gap analysis").
_NAME_FOLLOWER_NON_COMPANY = frozenset(
    {
        "cpa", "cph", "cpc", "cpm", "cost", "costs", "rate", "rates", "budget",
        "hires", "hire", "audience", "audiences", "market", "markets", "roles",
        "role", "analysis", "date", "dates", "goal", "goals", "range", "list",
    }
)


def _mentions(sentence: str, names: Iterable[str]) -> Optional[str]:
    """The first client-typed competitor name used AS A PROPER NOUN in
    ``sentence``: case-sensitive (the typed casing or ALL-CAPS) on word
    boundaries, so the common words "target", "gap", "apple", "delta",
    "visa", "shell" never match (verifier, 2026-10-01: "The hiring target of
    238..." was deleted for a plan that typed Target). A single-word name
    also does not count at the start of a sentence (capitalised there
    anyway) or directly before a metric word ("Target CPA sits at $7.50")."""
    for n in names:
        if not isinstance(n, str):
            continue
        n_clean = n.strip()
        if len(n_clean) < 2:
            continue
        single = len(n_clean.split()) == 1
        forms = {n_clean, n_clean.upper()}
        for form in forms:
            for m in re.finditer(
                rf"(?<![A-Za-z0-9]){re.escape(form)}(?![A-Za-z0-9])", sentence
            ):
                if single:
                    before = sentence[: m.start()]
                    if not re.search(r"[A-Za-z0-9]", re.sub(r"^\W*\w+:\s*", "", before)):
                        continue  # sentence-initial (after an optional "Label:")
                    nxt = re.match(r"\s+([A-Za-z]+)", sentence[m.end() :])
                    if nxt and (
                        nxt.group(1).lower() in _NAME_FOLLOWER_NON_COMPANY
                        or (nxt.group(1).isupper() and len(nxt.group(1)) <= 4)
                    ):
                        continue
                return n_clean
    return None


def strip_claim_sentences(
    text: Any,
    client_name: Any = "",
    unevidenced_names: Iterable[str] = (),
) -> Tuple[str, List[str]]:
    """Drop every sentence that (a) asserts behaviour about a named
    non-client company or (b) names one of ``unevidenced_names``. Paragraph
    breaks are preserved. Returns ``(clean_text, removed_sentences)``."""
    if not isinstance(text, str) or not text:
        return (text if isinstance(text, str) else ""), []
    names = [n for n in unevidenced_names if isinstance(n, str) and n.strip()]
    removed: List[str] = []
    out_paras: List[str] = []
    for para in text.split("\n"):
        if not para.strip():
            out_paras.append(para)
            continue
        kept: List[str] = []
        for sent in company_blurb.split_sentences(para):
            if find_asserted_claims(sent, client_name) or _mentions(sent, names):
                removed.append(sent)
            else:
                kept.append(sent)
        out_paras.append(" ".join(kept))
    clean = "\n".join(out_paras)
    clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
    return clean, removed


def competitor_has_evidence(entry: Any) -> bool:
    """A competitor entry is evidence-backed only when it carries sourced
    content: a non-generic ``description`` or an explicit evidence/source
    field. A bare typed name (the wizard's only shape) never is."""
    if not isinstance(entry, dict):
        return False
    for k in _EVIDENCE_KEYS:
        v = entry.get(k)
        if isinstance(v, str) and v.strip():
            return True
        if isinstance(v, (list, dict)) and v:
            return True
    desc = entry.get("description")
    if isinstance(desc, str):
        d = desc.strip().lower()
        if d not in _GENERIC_DESCRIPTIONS and not d.startswith("competing employer in"):
            return True
    return False


__all__ = [
    "ASSERTED_BEHAVIOR_PHRASES",
    "ASSERTED_BEHAVIOR_VERB_RES",
    "CLIENT_NAMED_LINE",
    "COMPETITOR_NAME_RE",
    "INFERRED_LINE",
    "NO_EVIDENCE_LINE",
    "NON_NAME_LEAD_WORDS",
    "competitor_has_evidence",
    "find_asserted_claims",
    "is_client_reference",
    "strip_claim_sentences",
]
