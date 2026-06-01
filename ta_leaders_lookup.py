"""TA-leader quote lookup for media plan generation.

Thin reader over ``data/ta_leaders_curated_2026.json`` (89 attributed posts
from top TA-leaders: Hung Lee, Madeline Mann, etc.). Plan-gen uses this to
populate "Trends to Watch" content with real attributed quotes instead of
generic prose.

Read-only; chatbot owns writes. All accessors use defensive ``.get()``.

Usage:
    from ta_leaders_lookup import get_top_quotes
    quotes = get_top_quotes(topic_filter={"programmatic_recruitment",
                                          "cpa_cpc_benchmarks"}, limit=3)
    for q in quotes:
        # q = {"name": ..., "title": ..., "quote": ..., "thesis": ...,
        #      "post_title": ..., "url": ..., "date": ...}
        print(f'"{q["quote"]}" -- {q["name"]}, {q["title"]}')
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

_DATA_PATH: Path = Path(__file__).parent / "data" / "ta_leaders_curated_2026.json"

# Topics most relevant to a recruitment media-plan audience. Used as default
# filter so plan-gen surfaces programmatic/benchmark content, not generic DEI
# or sourcing-tool noise.
DEFAULT_TOPICS: frozenset[str] = frozenset(
    {
        "programmatic_recruitment",
        "cpa_cpc_benchmarks",
        "ai_in_recruiting",
        "agentic_ai",
        "ta_tech_stack",
        "candidate_experience",
        "global_borderless_hiring",
        "fraud_detection",
    }
)

_data: dict[str, Any] = {}
_loaded: bool = False


def _load() -> dict[str, Any]:
    """Load and cache the TA-leaders JSON. Returns {} on any failure."""
    global _data, _loaded
    if _loaded:
        return _data
    _loaded = True
    try:
        if _DATA_PATH.exists():
            with open(_DATA_PATH, "r", encoding="utf-8") as f:
                _data = json.load(f)
            inf = _data.get("influencers") or []
            logger.info(
                "Loaded TA leaders from %s (%d influencers)",
                _DATA_PATH.name,
                len(inf),
            )
        else:
            logger.warning(
                "ta_leaders_curated_2026.json not found at %s -- "
                "trends quotes will be empty",
                _DATA_PATH,
            )
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to load TA leaders: %s", exc, exc_info=True)
        _data = {}
    return _data


def get_top_quotes(
    topic_filter: Iterable[str] | None = None,
    limit: int = 3,
    min_relevance: int = 8,
) -> list[dict[str, Any]]:
    """Return up to ``limit`` quotes from high-relevance TA leaders.

    Args:
        topic_filter: Iterable of topic taxonomy keys. A post matches if any
            of its ``topics`` overlap with this set. Defaults to
            ``DEFAULT_TOPICS`` (programmatic/CPA/AI focus).
        limit: Maximum quotes to return.
        min_relevance: Minimum ``joveo_relevance_score`` (0-10) on the
            influencer. 8 ≈ "directly relevant to programmatic recruitment".

    Returns:
        List of dicts: ``{name, title, quote, thesis, post_title, url, date,
        topic_matches}``. Sorted by relevance (highest first) then date
        (newest first). Empty list on miss or load failure. Never raises.
    """
    if topic_filter is None:
        topic_filter = DEFAULT_TOPICS
    topic_set = {t.strip().lower() for t in topic_filter if t}
    data = _load()
    influencers = data.get("influencers") or []
    if not influencers:
        return []

    rows: list[tuple[int, str, dict[str, Any]]] = []
    for inf in influencers:
        if not isinstance(inf, dict):
            continue
        relevance = inf.get("joveo_relevance_score")
        if not isinstance(relevance, (int, float)) or relevance < min_relevance:
            continue
        posts = inf.get("posts") or []
        if not isinstance(posts, list):
            continue
        for p in posts:
            if not isinstance(p, dict):
                continue
            quote = (p.get("key_quote") or "").strip()
            if not quote:
                continue
            post_topics = {str(t).strip().lower() for t in (p.get("topics") or [])}
            matches = topic_set & post_topics
            if not matches:
                continue
            row = {
                "name": inf.get("name") or "",
                "title": inf.get("title") or "",
                "quote": quote,
                "thesis": (p.get("thesis") or "").strip(),
                "post_title": p.get("title") or "",
                "url": p.get("url") or "",
                "date": p.get("date") or "",
                "topic_matches": sorted(matches),
            }
            # Sort key: relevance desc, then date desc
            rows.append((int(relevance), row["date"], row))

    # Sort: highest relevance first, then newest date
    rows.sort(key=lambda r: (r[0], r[1]), reverse=True)
    # Diversify: at most 1 quote per influencer so a single prolific voice
    # (e.g. Hung Lee) doesn't crowd out the panel.
    seen_names: set[str] = set()
    out: list[dict[str, Any]] = []
    for _, _, row in rows:
        name = row.get("name") or ""
        if name in seen_names:
            continue
        seen_names.add(name)
        out.append(row)
        if len(out) >= limit:
            break
    return out


def is_available() -> bool:
    data = _load()
    return bool(data.get("influencers"))
