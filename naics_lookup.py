"""US NAICS 2022 industry lookup + search (S94, NAICS typeahead).

Loads ``data/naics_2022.json`` once at import (error-isolated per project
convention -- see load_channels_db() in app.py) and exposes:

- ``naics_lookup(code)``  -> single code record + resolved internal_key, or None
- ``naics_search(q, limit)`` -> ranked matches for the wizard's typeahead

Resolution rule (per the data contract in data/naics_2022.json):
  1. Strip a ranged sector code (e.g. "31-33") to its first component ("31").
  2. Longest-prefix match against ``internal_key_map`` (checking progressively
     shorter prefixes of the numeric code).
  3. Fall back to ``default_internal_key`` if nothing matches.

Every one of the 2,125 codes is guaranteed to resolve to one of the 21/22
internal industry keys the benchmark stack keys off (shared_utils.INDUSTRY_LABEL_MAP)
-- this module never invents a new key and never returns an unresolved code.

Pure stdlib. No dependency on app.py (app.py imports this module, not the
other way around) so it stays free of circular-import risk.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_NAICS_PATH = os.path.join(_BASE_DIR, "data", "naics_2022.json")

_MAX_QUERY_LEN = 100

# Populated at import time; stay empty (never raise) if the data file is
# missing or malformed so a bad/missing NAICS dataset never blocks plan
# generation or server startup.
_CODES: List[Dict[str, Any]] = []
_BY_CODE: Dict[str, Dict[str, Any]] = {}
_INTERNAL_KEY_MAP: Dict[str, str] = {}
_DEFAULT_INTERNAL_KEY: str = "general_entry_level"
_LOADED: bool = False

try:
    with open(_NAICS_PATH, "r", encoding="utf-8") as _f:
        _raw = json.load(_f)
    _CODES = _raw.get("codes") or []
    _INTERNAL_KEY_MAP = _raw.get("internal_key_map") or {}
    _DEFAULT_INTERNAL_KEY = _raw.get("default_internal_key") or "general_entry_level"
    _BY_CODE = {c["code"]: c for c in _CODES if isinstance(c, dict) and c.get("code")}
    _LOADED = True
    logger.info(
        "Loaded %d NAICS 2022 codes (%s)", len(_CODES), _raw.get("version", "?")
    )
except (FileNotFoundError, json.JSONDecodeError, OSError, KeyError, TypeError) as e:
    logger.error("Failed to load data/naics_2022.json: %s", e, exc_info=True)
    _CODES = []
    _BY_CODE = {}
    _INTERNAL_KEY_MAP = {}
    _DEFAULT_INTERNAL_KEY = "general_entry_level"
    _LOADED = False


def _first_component(code: str) -> str:
    """Strip a ranged sector code ("31-33") down to its first component ("31")."""
    return code.split("-")[0] if "-" in code else code


def resolve_internal_key(code: str) -> str:
    """Resolve a NAICS code to one of the 21 internal industry keys.

    Longest-prefix match against internal_key_map; falls back to
    default_internal_key. Never raises, never returns an unmapped key.
    """
    if not code:
        return _DEFAULT_INTERNAL_KEY
    stripped = _first_component(str(code).strip())
    digits = re.sub(r"[^0-9]", "", stripped)
    if not digits:
        return _DEFAULT_INTERNAL_KEY
    for prefix_len in range(len(digits), 0, -1):
        prefix = digits[:prefix_len]
        if prefix in _INTERNAL_KEY_MAP:
            return _INTERNAL_KEY_MAP[prefix]
    return _DEFAULT_INTERNAL_KEY


def naics_lookup(code: str) -> Optional[Dict[str, Any]]:
    """Return {code, title, level, internal_key} for an exact NAICS code, or None."""
    if not code:
        return None
    record = _BY_CODE.get(str(code).strip())
    if not record:
        return None
    return {
        "code": record["code"],
        "title": record.get("title", ""),
        "level": record.get("level"),
        "internal_key": resolve_internal_key(record["code"]),
    }


def naics_search(q: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Ranked NAICS search for the wizard typeahead.

    Rank tiers (lower = better): 0 exact code, 1 code-prefix, 2 title
    startswith, 3 all-query-tokens present in title. Ties broken by
    preferring deeper (6-digit) codes, then shorter titles.
    """
    q = (q or "").strip()[:_MAX_QUERY_LEN]
    if not q or not _CODES:
        return []
    try:
        limit = max(1, min(int(limit), 50))
    except (TypeError, ValueError):
        limit = 20

    q_lower = q.lower()
    q_digits = re.sub(r"[^0-9]", "", q)
    tokens = [t for t in re.split(r"\s+", q_lower) if t]

    scored: List[tuple] = []
    for c in _CODES:
        code = c.get("code", "")
        title = c.get("title", "")
        title_lower = title.lower()
        rank: Optional[int] = None

        if q_digits and code == q_digits:
            rank = 0
        elif q_digits and code.startswith(q_digits):
            rank = 1
        elif title_lower == q_lower:
            rank = 0
        elif title_lower.startswith(q_lower):
            rank = 2
        elif tokens and all(t in title_lower for t in tokens):
            rank = 3

        if rank is not None:
            scored.append((rank, -int(c.get("level") or 0), len(title), c))

    scored.sort(key=lambda item: (item[0], item[1], item[2]))

    results: List[Dict[str, Any]] = []
    for _rank, _neg_level, _title_len, c in scored[:limit]:
        results.append(
            {
                "code": c.get("code", ""),
                "title": c.get("title", ""),
                "level": c.get("level"),
                "internal_key": resolve_internal_key(c.get("code", "")),
            }
        )
    return results


def is_loaded() -> bool:
    """Whether the NAICS dataset loaded successfully (used by health checks/tests)."""
    return _LOADED
