"""Lightcast Open Skills lookup (apis.data).

Source : https://lightcast.io/open-skills
License: CC BY 4.0 (attribution required when displayed downstream)
Refresh: bi-weekly (on Lightcast's side; this module re-loads on cache reset).

Public functions:
    - ``lookup_lightcast_skill(skill, *, fuzzy, limit, category, timeout)``
    - ``lookup_lightcast_occupation(title, *, fuzzy, limit, timeout)``

Data source policy
==================
Lightcast does **not** publish a free bulk download URL as of May 2026; their
data sits behind ``docs.lightcast.dev`` (Skills + Titles APIs, registration
required). To keep this module free + stdlib-only:

* If ``data/lightcast_open_skills.{json,csv}`` exists, the module loads it,
  builds an in-memory index, and returns matches.
* If no local file is present, the module returns a graceful error dict
  pointing the user at https://lightcast.io/open-skills with instructions
  for placing the file -- no crash, no silent failure.
* The same dual policy applies to ``data/lightcast_open_titles.{json,csv}``
  for the occupation lookup.

Accepted file shapes
====================
JSON (preferred -- matches the Skills API response shape)::

    [
        {"id": "KS123...", "name": "Python", "category": "...",
         "type": "Hard Skill" | "Soft Skill" | "Certification" | ...},
        ...
    ]

CSV (header row required)::

    id,name,category,type
    KS123...,Python,Information Technology,Hard Skill

Coding contract follows the project's Python rules: type hints, f-strings,
specific exceptions, ``logger.error(exc_info=True)``, graceful degradation.
"""

from __future__ import annotations

import csv
import difflib
import json
import logging
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ─── Module constants ─────────────────────────────────────────────────────────

SOURCE_NAME = "Lightcast Open Skills (2026 release)"
SOURCE_TITLES_NAME = "Lightcast Open Titles (2026 release)"
LICENSE = "CC BY 4.0 (attribution required)"

# Where users can request the dataset (no direct download URL is publicly
# available -- the data is behind docs.lightcast.dev authentication).
DOWNLOAD_GUIDANCE_URL = "https://lightcast.io/open-skills"
TITLES_GUIDANCE_URL = "https://docs.lightcast.dev/apis/titles"

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = _PROJECT_ROOT / "data"

# Supported file paths (first present wins).
_SKILL_PATHS = (
    _DATA_DIR / "lightcast_open_skills.json",
    _DATA_DIR / "lightcast_open_skills.csv",
)
_TITLE_PATHS = (
    _DATA_DIR / "lightcast_open_titles.json",
    _DATA_DIR / "lightcast_open_titles.csv",
)

_REQUIRED_FIELDS = ("id", "name")
# CSV/JSON keys we surface in matches; missing keys default to "".
_OUTPUT_FIELDS = ("id", "name", "category", "type")

# Concurrency guards: parsed indices are cached in module-level dicts.
_SKILL_LOCK = threading.Lock()
_TITLE_LOCK = threading.Lock()
_SKILL_INDEX: list[dict[str, str]] | None = None
_TITLE_INDEX: list[dict[str, str]] | None = None
_SKILL_LOAD_ERROR: str | None = None
_TITLE_LOAD_ERROR: str | None = None


# ─── Loaders ──────────────────────────────────────────────────────────────────


def _resolve_path(candidates: tuple[Path, ...]) -> Path | None:
    """Return the first existing, non-empty path from ``candidates``."""
    for path in candidates:
        try:
            if path.exists() and path.stat().st_size > 0:
                return path
        except OSError:
            # Permissions / IO race -- skip and try the next candidate.
            logger.warning(f"Could not stat {path}", exc_info=True)
    return None


def _load_records(path: Path) -> list[dict[str, str]]:
    """Load records from JSON (list[dict]) or CSV; normalize to dict[str,str].

    Raises:
        OSError, json.JSONDecodeError, csv.Error, ValueError, KeyError.
    """
    suffix = path.suffix.lower()
    if suffix == ".json":
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, list):
            # Some Lightcast snapshots wrap the array under a "data" key.
            if isinstance(payload, dict):
                inner = payload.get("data") or payload.get("results")
                if isinstance(inner, list):
                    payload = inner
                else:
                    raise ValueError(
                        f"{path.name}: expected a JSON list or dict.data list"
                    )
            else:
                raise ValueError(f"{path.name}: expected a JSON list")
        rows = [_coerce_record(item) for item in payload if isinstance(item, dict)]
    elif suffix == ".csv":
        with open(path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                raise csv.Error(f"{path.name}: header row missing")
            rows = [_coerce_record(item) for item in reader]
    else:
        raise ValueError(f"unsupported file extension: {path.suffix}")

    # Filter out malformed rows (must have id + name).
    return [r for r in rows if all(r.get(f) for f in _REQUIRED_FIELDS)]


def _coerce_record(item: dict[str, Any]) -> dict[str, str]:
    """Normalize a Lightcast record into ``{id, name, category, type}`` strings.

    Lightcast variants seen in the wild:
        - ``{"id": "...", "name": "...", "type": {"name": "Hard Skill"}}``
        - ``{"id": "...", "name": "...", "category": {"name": "IT"}}``
        - flat strings on category/type
    """
    out: dict[str, str] = {}
    out["id"] = (item.get("id") or item.get("Id") or "").strip()
    out["name"] = (item.get("name") or item.get("Name") or "").strip()

    category = item.get("category") or item.get("Category") or ""
    if isinstance(category, dict):
        category = category.get("name") or category.get("Name") or ""
    out["category"] = (category or "").strip() if isinstance(category, str) else ""

    type_ = item.get("type") or item.get("Type") or ""
    if isinstance(type_, dict):
        type_ = type_.get("name") or type_.get("Name") or ""
    out["type"] = (type_ or "").strip() if isinstance(type_, str) else ""
    return out


def _load_skill_index() -> list[dict[str, str]]:
    """Load + cache the skills index, raising on hard failures."""
    global _SKILL_INDEX, _SKILL_LOAD_ERROR
    if _SKILL_INDEX is not None:
        return _SKILL_INDEX
    with _SKILL_LOCK:
        if _SKILL_INDEX is not None:
            return _SKILL_INDEX
        if _SKILL_LOAD_ERROR is not None:
            raise OSError(_SKILL_LOAD_ERROR)
        path = _resolve_path(_SKILL_PATHS)
        if path is None:
            msg = (
                f"Lightcast Open Skills data not available; download from "
                f"{DOWNLOAD_GUIDANCE_URL} and place at "
                f"data/lightcast_open_skills.csv (or .json)"
            )
            _SKILL_LOAD_ERROR = msg
            raise FileNotFoundError(msg)
        try:
            records = _load_records(path)
        except (OSError, json.JSONDecodeError, csv.Error, ValueError, KeyError) as exc:
            _SKILL_LOAD_ERROR = (
                f"Failed to parse {path.name}: {type(exc).__name__}: {exc}"
            )
            raise
        if not records:
            _SKILL_LOAD_ERROR = f"{path.name}: contained zero valid records"
            raise ValueError(_SKILL_LOAD_ERROR)
        logger.info(f"Loaded {len(records)} Lightcast skills from {path}")
        _SKILL_INDEX = records
        return _SKILL_INDEX


def _load_title_index() -> list[dict[str, str]]:
    """Load + cache the occupation/titles index, raising on hard failures."""
    global _TITLE_INDEX, _TITLE_LOAD_ERROR
    if _TITLE_INDEX is not None:
        return _TITLE_INDEX
    with _TITLE_LOCK:
        if _TITLE_INDEX is not None:
            return _TITLE_INDEX
        if _TITLE_LOAD_ERROR is not None:
            raise OSError(_TITLE_LOAD_ERROR)
        path = _resolve_path(_TITLE_PATHS)
        if path is None:
            msg = (
                f"Lightcast Open Titles data not available; download from "
                f"{TITLES_GUIDANCE_URL} and place at "
                f"data/lightcast_open_titles.csv (or .json)"
            )
            _TITLE_LOAD_ERROR = msg
            raise FileNotFoundError(msg)
        try:
            records = _load_records(path)
        except (OSError, json.JSONDecodeError, csv.Error, ValueError, KeyError) as exc:
            _TITLE_LOAD_ERROR = (
                f"Failed to parse {path.name}: {type(exc).__name__}: {exc}"
            )
            raise
        if not records:
            _TITLE_LOAD_ERROR = f"{path.name}: contained zero valid records"
            raise ValueError(_TITLE_LOAD_ERROR)
        logger.info(f"Loaded {len(records)} Lightcast titles from {path}")
        _TITLE_INDEX = records
        return _TITLE_INDEX


def reset_cache_for_tests() -> None:
    """Clear the in-memory indices (test hook only)."""
    global _SKILL_INDEX, _TITLE_INDEX, _SKILL_LOAD_ERROR, _TITLE_LOAD_ERROR
    with _SKILL_LOCK:
        _SKILL_INDEX = None
        _SKILL_LOAD_ERROR = None
    with _TITLE_LOCK:
        _TITLE_INDEX = None
        _TITLE_LOAD_ERROR = None


# ─── Matching ─────────────────────────────────────────────────────────────────


def _slim(record: dict[str, str]) -> dict[str, str]:
    """Trim a record to the public output shape."""
    return {f: record.get(f) or "" for f in _OUTPUT_FIELDS}


def _filter_by_category(
    records: list[dict[str, str]], category: str | None
) -> list[dict[str, str]]:
    """Case-insensitive substring filter on the ``category`` field."""
    if not category:
        return records
    needle = category.strip().lower()
    return [r for r in records if needle in (r.get("category") or "").lower()]


def _exact_or_substring(
    records: list[dict[str, str]],
    query: str,
    limit: int,
) -> list[dict[str, str]]:
    """Rank exact-match first, then substring matches; cap at ``limit``."""
    needle = query.strip().lower()
    exact: list[dict[str, str]] = []
    substr: list[dict[str, str]] = []
    for record in records:
        name_lower = (record.get("name") or "").lower()
        if name_lower == needle:
            exact.append(record)
        elif needle in name_lower:
            substr.append(record)
        if len(exact) >= limit:
            break
    combined = exact + substr
    return combined[:limit]


def _fuzzy(
    records: list[dict[str, str]],
    query: str,
    limit: int,
) -> list[dict[str, str]]:
    """Levenshtein-ish fuzzy match via stdlib ``difflib.get_close_matches``."""
    by_name: dict[str, dict[str, str]] = {}
    for record in records:
        name = record.get("name") or ""
        if name:
            # First record wins on name collision (rare; Lightcast IDs differ).
            by_name.setdefault(name.lower(), record)
    matches = difflib.get_close_matches(
        query.strip().lower(), by_name.keys(), n=limit, cutoff=0.6
    )
    return [by_name[m] for m in matches]


def _search(
    records: list[dict[str, str]],
    query: str,
    fuzzy: bool,
    limit: int,
    category: str | None = None,
) -> list[dict[str, str]]:
    """Combined exact + substring + (optionally) fuzzy search."""
    if limit <= 0:
        return []
    pool = _filter_by_category(records, category)
    primary = _exact_or_substring(pool, query, limit)
    if len(primary) >= limit or not fuzzy:
        return [_slim(r) for r in primary]
    seen_ids = {r.get("id") for r in primary}
    fuzzy_hits = [
        r
        for r in _fuzzy(pool, query, limit - len(primary))
        if r.get("id") not in seen_ids
    ]
    return [_slim(r) for r in primary + fuzzy_hits][:limit]


# ─── Public entrypoints ───────────────────────────────────────────────────────


def _err(message: str, source: str) -> dict[str, Any]:
    """Uniform error envelope."""
    return {"error": message, "source": source}


def lookup_lightcast_skill(
    skill: str,
    *,
    fuzzy: bool = True,
    limit: int = 10,
    category: str | None = None,
    timeout: int = 30,  # noqa: ARG001 -- accepted for API parity (no remote fetch)
) -> dict[str, Any]:
    """Look up skills in the Lightcast Open Skills taxonomy.

    Args:
        skill: Free-text skill query (e.g. ``"python"``, ``"machine learning"``).
        fuzzy: When True, fall back to ``difflib`` close-match scoring after
            exact and substring matches are exhausted.
        limit: Max matches returned (clamped to 1-50).
        category: Optional case-insensitive substring filter on the
            ``category`` field (e.g. ``"information technology"``).
        timeout: Reserved for symmetry with ``recruitment_apis``; the bulk
            data is loaded from disk so this argument is unused today but is
            retained so future remote-fetch additions don't break the
            signature.

    Returns:
        On success::

            {
                "query": str,
                "matches": [{"id","name","category","type"}, ...],
                "source": "Lightcast Open Skills (2026 release)",
                "license": "CC BY 4.0 (attribution required)",
            }

        On data-not-available or parse failure::

            {"error": str, "source": "Lightcast Open Skills (2026 release)"}
    """
    if not skill or not skill.strip():
        return _err("skill must be non-empty", SOURCE_NAME)
    capped_limit = max(1, min(int(limit) if limit else 10, 50))

    try:
        records = _load_skill_index()
    except (FileNotFoundError, OSError) as exc:
        # FileNotFoundError = data not yet provisioned; preserve the helpful
        # guidance message verbatim. Other OSErrors include parse failures.
        logger.error("Lightcast skills index unavailable", exc_info=True)
        return _err(str(exc), SOURCE_NAME)
    except (json.JSONDecodeError, csv.Error, ValueError, KeyError) as exc:
        logger.error("Lightcast skills index parse failed", exc_info=True)
        return _err(f"data parse failed: {type(exc).__name__}: {exc}", SOURCE_NAME)

    matches = _search(
        records, skill, fuzzy=fuzzy, limit=capped_limit, category=category
    )
    return {
        "query": skill,
        "matches": matches,
        "source": SOURCE_NAME,
        "license": LICENSE,
    }


def lookup_lightcast_occupation(
    title: str,
    *,
    fuzzy: bool = True,
    limit: int = 5,
    timeout: int = 30,  # noqa: ARG001 -- API parity, see lookup_lightcast_skill
) -> dict[str, Any]:
    """Look up occupations in the Lightcast Open Titles taxonomy.

    Args:
        title: Free-text job title query (e.g. ``"software engineer"``).
        fuzzy: Enable difflib fallback after exact/substring matches.
        limit: Max matches returned (clamped to 1-25).
        timeout: Reserved for API parity (see ``lookup_lightcast_skill``).

    Returns:
        Same shape as ``lookup_lightcast_skill`` but with the
        ``Lightcast Open Titles`` source name.
    """
    if not title or not title.strip():
        return _err("title must be non-empty", SOURCE_TITLES_NAME)
    capped_limit = max(1, min(int(limit) if limit else 5, 25))

    try:
        records = _load_title_index()
    except (FileNotFoundError, OSError) as exc:
        logger.error("Lightcast titles index unavailable", exc_info=True)
        return _err(str(exc), SOURCE_TITLES_NAME)
    except (json.JSONDecodeError, csv.Error, ValueError, KeyError) as exc:
        logger.error("Lightcast titles index parse failed", exc_info=True)
        return _err(
            f"data parse failed: {type(exc).__name__}: {exc}",
            SOURCE_TITLES_NAME,
        )

    matches = _search(records, title, fuzzy=fuzzy, limit=capped_limit)
    return {
        "query": title,
        "matches": matches,
        "source": SOURCE_TITLES_NAME,
        "license": LICENSE,
    }


__all__ = [
    "lookup_lightcast_skill",
    "lookup_lightcast_occupation",
    "SOURCE_NAME",
    "SOURCE_TITLES_NAME",
    "LICENSE",
    "DOWNLOAD_GUIDANCE_URL",
    "TITLES_GUIDANCE_URL",
    "reset_cache_for_tests",
]
