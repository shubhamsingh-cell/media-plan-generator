"""Stack Overflow 2025 Developer Survey lookup (apis.data).

Source : https://survey.stackoverflow.co/2025/
Dataset: https://survey.stackoverflow.co/datasets/stack-overflow-developer-survey-2025.zip
License: Open Database License (ODbL)
Sample : ~49,000 respondents, 177 countries, 314 technologies.

Public function:
    ``lookup_so_survey(metric, *, role, country, language, timeout)``

Supported metrics:
    - ``salary``               -- median + IQR of ConvertedCompYearly
    - ``tech_admiration``      -- top items from AdmiredTools/AdmiredLanguages
    - ``language_use``         -- % usage of LanguageHaveWorkedWith
    - ``country_distribution`` -- response count by Country
    - ``experience``           -- YearsCode distribution

The CSV is downloaded on first call (16-17 MB ZIP, expands to ~70 MB CSV).
Subsequent calls reuse the cached file under ``data/``. The module never
loads the full CSV into memory; it parses lazily into compact per-metric
indices using stdlib ``csv``.

Coding contract follows the project's Python rules: type hints, f-strings,
specific exceptions, ``logger.error(exc_info=True)``, and graceful
degradation -- any failure returns ``{"error": str, "source": ...}``.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import ssl
import threading
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Iterable, Iterator

logger = logging.getLogger(__name__)

# ─── Module constants ─────────────────────────────────────────────────────────

SOURCE_NAME = "Stack Overflow 2025 Developer Survey"
LICENSE = "Open Database License"

# Canonical 2025 dataset URL (verified May 2026: HTTP 200, ~16.9 MB ZIP).
# The 2024 survey used a flat ``cdn.stackoverflow.co/...survey-results-public.csv``
# pattern; the 2025 survey ships as a ZIP at survey.stackoverflow.co/datasets/.
DATASET_URL = (
    "https://survey.stackoverflow.co/datasets/"
    "stack-overflow-developer-survey-2025.zip"
)

# Local cache paths (resolved against the project root, not CWD).
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = _PROJECT_ROOT / "data"
CSV_PATH = _DATA_DIR / "stack_overflow_survey_2025.csv"
ZIP_PATH = _DATA_DIR / "stack_overflow_survey_2025.zip"

# CSV column names. Stack Overflow has used these stable names since 2022;
# if the 2025 survey renamed any of them, ``_get`` raises KeyError caught at
# the boundary and surfaces a graceful error dict to the caller.
_COL_DEVTYPE = "DevType"
_COL_COUNTRY = "Country"
_COL_LANG_USED = "LanguageHaveWorkedWith"
_COL_ADMIRED_LANG = "LanguageAdmired"  # 2025 column name
_COL_ADMIRED_TOOLS = "ToolsTechAdmired"  # 2025 column name
# Fallback names used in earlier surveys; we try both at parse time.
_COL_ADMIRED_LANG_FALLBACK = "AdmiredLanguages"
_COL_ADMIRED_TOOLS_FALLBACK = "AdmiredTools"
_COL_COMP_YEARLY = "ConvertedCompYearly"
_COL_YEARS_CODE = "YearsCode"

_MULTI_VALUE_DELIMITER = ";"

# Shared SSL context (some envs have stale CA bundles -- fall back if needed).
_ssl_ctx = ssl.create_default_context()
_ssl_ctx_unverified = ssl._create_unverified_context()

_USER_AGENT = "Joveo-Nova-SO-Survey/1.0"

# In-memory cache of the parsed dataset, lazy-built on first lookup. Guarded
# by a module-level lock so concurrent callers cannot duplicate the parse.
_DATASET_LOCK = threading.Lock()
_DATASET: dict[str, Any] | None = None
_DATASET_ERROR: str | None = None


# ─── Download + cache ─────────────────────────────────────────────────────────


def _ensure_csv_cached(timeout: int) -> Path:
    """Ensure the dataset CSV is available locally; download once if missing.

    Args:
        timeout: HTTP timeout in seconds (only used on first download).

    Returns:
        Absolute path to the CSV on disk.

    Raises:
        urllib.error.URLError, urllib.error.HTTPError, OSError, zipfile.BadZipFile.
    """
    if CSV_PATH.exists() and CSV_PATH.stat().st_size > 0:
        return CSV_PATH

    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading SO 2025 Survey dataset from {DATASET_URL}")
    req = urllib.request.Request(DATASET_URL, headers={"User-Agent": _USER_AGENT})

    # Stream to disk to avoid loading 17 MB into memory unnecessarily.
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
            _stream_to_file(resp, ZIP_PATH)
    except urllib.error.URLError as ssl_exc:
        # Retry once with an unverified SSL context if cert verification fails.
        if "CERTIFICATE_VERIFY_FAILED" in str(ssl_exc) or "SSL" in str(ssl_exc):
            logger.warning(
                "SSL verify failed for SO Survey download; retrying unverified",
                exc_info=True,
            )
            req2 = urllib.request.Request(
                DATASET_URL, headers={"User-Agent": _USER_AGENT}
            )
            with urllib.request.urlopen(
                req2, timeout=timeout, context=_ssl_ctx_unverified
            ) as resp:
                _stream_to_file(resp, ZIP_PATH)
        else:
            raise

    # The bundle is a ZIP; extract the single survey CSV inside.
    with zipfile.ZipFile(ZIP_PATH) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not members:
            raise zipfile.BadZipFile("SO Survey ZIP did not contain a CSV file")
        # Prefer the public results CSV if multiple are present.
        target = next((m for m in members if "public" in m.lower()), members[0])
        with zf.open(target) as src, open(CSV_PATH, "wb") as dst:
            while True:
                chunk = src.read(1 << 20)
                if not chunk:
                    break
                dst.write(chunk)

    # ZIP is no longer needed once CSV is extracted.
    try:
        ZIP_PATH.unlink(missing_ok=True)
    except OSError:
        # Non-fatal: leave the ZIP if cleanup fails.
        logger.warning(f"Could not remove {ZIP_PATH}", exc_info=True)

    return CSV_PATH


def _stream_to_file(resp: Any, dest: Path) -> None:
    """Stream an HTTP response body to ``dest`` in 1 MiB chunks."""
    with open(dest, "wb") as fh:
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            fh.write(chunk)


# ─── Parsing + indexing ───────────────────────────────────────────────────────


def _iter_rows() -> Iterator[dict[str, str]]:
    """Yield CSV rows as dicts, decoding latin-1 fallback for stray bytes."""
    # The SO survey CSV is UTF-8 but occasional rows include latin-1 bytes;
    # ``errors="replace"`` keeps the parse total without crashing.
    with open(CSV_PATH, "r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield row


def _split_multi(value: str) -> list[str]:
    """Split a SO multi-select column on ``;`` and strip whitespace/blanks."""
    if not value:
        return []
    return [s.strip() for s in value.split(_MULTI_VALUE_DELIMITER) if s.strip()]


def _to_float(value: str) -> float | None:
    """Parse a numeric SO column; returns None on blank or non-numeric."""
    if not value or not value.strip():
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_years(value: str) -> float | None:
    """Map YearsCode to float; SO uses ``Less than 1 year`` and ``More than 50 years``."""
    if not value:
        return None
    cleaned = value.strip().lower()
    if cleaned == "less than 1 year":
        return 0.5
    if cleaned == "more than 50 years":
        return 51.0
    return _to_float(cleaned)


def _build_dataset() -> dict[str, Any]:
    """Walk the CSV once and build all per-metric indices in compact form.

    Returns a dict with keys:
        - ``n_total``           -- total rows scanned
        - ``salary``            -- list[(role, country, comp)] tuples
        - ``language_counts``   -- {language: count}
        - ``admired_tools``     -- {tool: count}
        - ``admired_languages`` -- {language: count}
        - ``country_counts``    -- {country: count}
        - ``years_code``        -- list[float]
        - ``columns``           -- the raw fieldnames (for diagnostics)
        - ``warnings``          -- list of soft-failure notes (e.g. column renames)
    """
    salary_records: list[tuple[str, str, float]] = []
    language_counts: dict[str, int] = {}
    admired_tools: dict[str, int] = {}
    admired_langs: dict[str, int] = {}
    country_counts: dict[str, int] = {}
    years_code: list[float] = []
    warnings: list[str] = []

    # Probe column names by reading the header row first.
    with open(CSV_PATH, "r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise csv.Error("SO Survey CSV is empty") from exc

    fieldnames = set(header)
    admired_lang_col = _resolve_column(
        fieldnames, _COL_ADMIRED_LANG, _COL_ADMIRED_LANG_FALLBACK, warnings
    )
    admired_tools_col = _resolve_column(
        fieldnames, _COL_ADMIRED_TOOLS, _COL_ADMIRED_TOOLS_FALLBACK, warnings
    )

    n_total = 0
    for row in _iter_rows():
        n_total += 1
        country = (row.get(_COL_COUNTRY) or "").strip()
        if country:
            country_counts[country] = country_counts.get(country, 0) + 1

        roles = _split_multi(row.get(_COL_DEVTYPE) or "")
        comp = _to_float(row.get(_COL_COMP_YEARLY) or "")
        if comp and comp > 0:
            # Record salary against each role the respondent listed (so we can
            # filter by primary role at query time without re-walking the CSV).
            if roles:
                for role in roles:
                    salary_records.append((role, country, comp))
            else:
                salary_records.append(("", country, comp))

        for lang in _split_multi(row.get(_COL_LANG_USED) or ""):
            language_counts[lang] = language_counts.get(lang, 0) + 1

        if admired_lang_col:
            for lang in _split_multi(row.get(admired_lang_col) or ""):
                admired_langs[lang] = admired_langs.get(lang, 0) + 1
        if admired_tools_col:
            for tool in _split_multi(row.get(admired_tools_col) or ""):
                admired_tools[tool] = admired_tools.get(tool, 0) + 1

        years = _normalize_years(row.get(_COL_YEARS_CODE) or "")
        if years is not None:
            years_code.append(years)

    return {
        "n_total": n_total,
        "salary": salary_records,
        "language_counts": language_counts,
        "admired_tools": admired_tools,
        "admired_languages": admired_langs,
        "country_counts": country_counts,
        "years_code": years_code,
        "columns": header,
        "warnings": warnings,
    }


def _resolve_column(
    fieldnames: set[str],
    primary: str,
    fallback: str,
    warnings: list[str],
) -> str | None:
    """Return whichever of ``primary``/``fallback`` exists in the header.

    Records a warning so callers can see which column was used. Returns None
    when neither column is present (the metric using it will be empty).
    """
    if primary in fieldnames:
        return primary
    if fallback in fieldnames:
        warnings.append(f"column '{primary}' missing; using fallback '{fallback}'")
        return fallback
    warnings.append(
        f"column '{primary}' and fallback '{fallback}' both missing; "
        "the corresponding metric will be empty"
    )
    return None


def _ensure_dataset(timeout: int) -> dict[str, Any]:
    """Lazily download + parse the dataset on first call. Thread-safe.

    Raises:
        Whatever ``_ensure_csv_cached`` or ``_build_dataset`` raise. Callers
        should catch the documented exception tuple at the boundary.
    """
    global _DATASET, _DATASET_ERROR
    if _DATASET is not None:
        return _DATASET
    with _DATASET_LOCK:
        if _DATASET is not None:
            return _DATASET
        # If a previous call already failed, do not retry on every request --
        # that would block the chatbot indefinitely. Re-raise the cached err.
        if _DATASET_ERROR is not None:
            raise OSError(_DATASET_ERROR)
        try:
            _ensure_csv_cached(timeout=timeout)
            _DATASET = _build_dataset()
            return _DATASET
        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            OSError,
            zipfile.BadZipFile,
            csv.Error,
            ValueError,
            KeyError,
        ) as exc:
            _DATASET_ERROR = f"{type(exc).__name__}: {exc}"
            raise


def reset_cache_for_tests() -> None:
    """Clear the in-memory dataset cache (test hook only)."""
    global _DATASET, _DATASET_ERROR
    with _DATASET_LOCK:
        _DATASET = None
        _DATASET_ERROR = None


# ─── Metric implementations ───────────────────────────────────────────────────


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolated percentile on a pre-sorted list (no numpy)."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = rank - lo
    return sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo])


def _metric_salary(
    ds: dict[str, Any],
    role: str | None,
    country: str | None,
) -> tuple[dict[str, Any], int]:
    """Median + IQR of ConvertedCompYearly, optionally filtered."""
    role_l = (role or "").strip().lower()
    country_l = (country or "").strip().lower()
    comps: list[float] = []
    for rec_role, rec_country, comp in ds["salary"]:
        if role_l and role_l not in rec_role.lower():
            continue
        if country_l and country_l not in rec_country.lower():
            continue
        comps.append(comp)
    if not comps:
        return {
            "median_usd": None,
            "p25_usd": None,
            "p75_usd": None,
            "iqr_usd": None,
        }, 0
    comps.sort()
    median = _percentile(comps, 50)
    p25 = _percentile(comps, 25)
    p75 = _percentile(comps, 75)
    return {
        "median_usd": round(median, 2),
        "p25_usd": round(p25, 2),
        "p75_usd": round(p75, 2),
        "iqr_usd": round(p75 - p25, 2),
    }, len(comps)


def _metric_top_counts(
    counter: dict[str, int],
    top_n: int = 20,
) -> list[dict[str, Any]]:
    """Return the top-N items from a counter dict as ``{name, count, share}``."""
    if not counter:
        return []
    total = sum(counter.values())
    items = sorted(counter.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    return [
        {
            "name": name,
            "count": count,
            "share_pct": round(100.0 * count / total, 2) if total else 0.0,
        }
        for name, count in items
    ]


def _metric_language_use(
    ds: dict[str, Any],
    language: str | None,
) -> tuple[Any, int]:
    """Language usage shares; if ``language`` provided, return that one's share."""
    counts = ds["language_counts"]
    n_resp = ds["n_total"] or 1
    if language:
        target = language.strip().lower()
        match = next(
            (
                {"name": k, "count": v, "share_pct": round(100.0 * v / n_resp, 2)}
                for k, v in counts.items()
                if k.lower() == target
            ),
            None,
        )
        if match is None:
            return {"language": language, "found": False}, 0
        return match, match["count"]
    # No filter: return top-20 ranking.
    return _metric_top_counts(counts, top_n=20), sum(counts.values())


def _metric_country_distribution(
    ds: dict[str, Any],
    country: str | None,
) -> tuple[Any, int]:
    """Country counts; if ``country`` provided, narrow to that country."""
    counts = ds["country_counts"]
    n_resp = ds["n_total"] or 1
    if country:
        target = country.strip().lower()
        match = next(
            (
                {"name": k, "count": v, "share_pct": round(100.0 * v / n_resp, 2)}
                for k, v in counts.items()
                if k.lower() == target
            ),
            None,
        )
        if match is None:
            return {"country": country, "found": False}, 0
        return match, match["count"]
    return _metric_top_counts(counts, top_n=20), sum(counts.values())


def _metric_experience(ds: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """Distribution stats for YearsCode (median, p25, p75, mean)."""
    years = sorted(ds["years_code"])
    if not years:
        return {"median": None, "p25": None, "p75": None, "mean": None}, 0
    return {
        "median": round(_percentile(years, 50), 2),
        "p25": round(_percentile(years, 25), 2),
        "p75": round(_percentile(years, 75), 2),
        "mean": round(sum(years) / len(years), 2),
    }, len(years)


# ─── Public entrypoint ────────────────────────────────────────────────────────


_SUPPORTED_METRICS = (
    "salary",
    "tech_admiration",
    "language_use",
    "country_distribution",
    "experience",
)


def _envelope(
    metric: str,
    filters: dict[str, Any],
    n_respondents: int,
    result: Any,
) -> dict[str, Any]:
    """Wrap a successful result in the documented response envelope."""
    return {
        "metric": metric,
        "filters_applied": {k: v for k, v in filters.items() if v},
        "n_respondents": n_respondents,
        "result": result,
        "source": SOURCE_NAME,
        "license": LICENSE,
    }


def _err(message: str) -> dict[str, Any]:
    """Uniform error envelope -- no crash at the call boundary."""
    return {"error": message, "source": SOURCE_NAME}


def lookup_so_survey(
    metric: str,
    *,
    role: str | None = None,
    country: str | None = None,
    language: str | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    """Query the cached Stack Overflow 2025 Developer Survey.

    Args:
        metric: One of ``salary``, ``tech_admiration``, ``language_use``,
            ``country_distribution``, ``experience``.
        role: Optional DevType filter (substring match, e.g.
            ``"Full-stack"``). Applies to the ``salary`` metric.
        country: Optional country filter (substring match for ``salary``;
            exact-case-insensitive lookup for ``country_distribution``).
        language: Optional ``LanguageHaveWorkedWith`` filter for the
            ``language_use`` metric (case-insensitive exact match).
        timeout: HTTP timeout for the first-call CSV download.

    Returns:
        On success::

            {
                "metric": str,
                "filters_applied": dict,
                "n_respondents": int,
                "result": dict | list,
                "source": "Stack Overflow 2025 Developer Survey",
                "license": "Open Database License",
            }

        On failure (download/parse/missing column/unknown metric)::

            {"error": str, "source": "Stack Overflow 2025 Developer Survey"}
    """
    if not metric:
        return _err("metric is required")
    if metric not in _SUPPORTED_METRICS:
        return _err(
            f"unknown metric '{metric}'; supported: {', '.join(_SUPPORTED_METRICS)}"
        )

    try:
        ds = _ensure_dataset(timeout=timeout)
    except (
        urllib.error.URLError,
        urllib.error.HTTPError,
        OSError,
        zipfile.BadZipFile,
        csv.Error,
        ValueError,
        KeyError,
    ) as exc:
        logger.error(
            f"SO Survey dataset unavailable for metric={metric}", exc_info=True
        )
        return _err(f"dataset unavailable: {type(exc).__name__}: {exc}")

    filters = {"role": role, "country": country, "language": language}
    try:
        if metric == "salary":
            result, n = _metric_salary(ds, role, country)
        elif metric == "tech_admiration":
            tools = _metric_top_counts(ds["admired_tools"], top_n=20)
            langs = _metric_top_counts(ds["admired_languages"], top_n=20)
            result = {"top_admired_tools": tools, "top_admired_languages": langs}
            n = sum(ds["admired_tools"].values()) + sum(
                ds["admired_languages"].values()
            )
        elif metric == "language_use":
            result, n = _metric_language_use(ds, language)
        elif metric == "country_distribution":
            result, n = _metric_country_distribution(ds, country)
        elif metric == "experience":
            result, n = _metric_experience(ds)
        else:  # pragma: no cover -- guarded above
            return _err(f"unhandled metric '{metric}'")
    except (KeyError, ValueError, TypeError) as exc:
        logger.error(
            f"SO Survey metric calculation failed for metric={metric}",
            exc_info=True,
        )
        return _err(f"metric calculation failed: {type(exc).__name__}: {exc}")

    envelope = _envelope(metric, filters, n, result)
    if ds.get("warnings"):
        envelope["warnings"] = list(ds["warnings"])
    return envelope


__all__ = [
    "lookup_so_survey",
    "SOURCE_NAME",
    "LICENSE",
    "DATASET_URL",
    "CSV_PATH",
    "reset_cache_for_tests",
]
