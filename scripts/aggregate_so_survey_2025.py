"""Aggregate Stack Overflow 2025 Developer Survey into JSON summaries.

S50 (May 2026): One-shot aggregator that turns the 140 MB raw CSV into a
small JSON file (~200 KB) suitable for fast Nova chatbot lookups. Run once
after `data/so_survey_2025/` has been populated by the survey ZIP.

Output: data/so_survey_2025_aggregates.json with the following structure:

{
  "metadata": {
    "source": "Stack Overflow 2025 Developer Survey",
    "license": "Open Database License (ODbL)",
    "n_respondents": int,
    "generated_at": ISO8601,
    "survey_url": str,
  },
  "country_counts": {country: count},
  "devtype_counts": {devtype: count},
  "yearscode_distribution": {bucket: count},
  "salary_by_country_devtype_usd": {
    "country|devtype": {"n": int, "p25": float, "p50": float, "p75": float, "mean": float}
  },
  "salary_by_country_usd": {country: {n, p25, p50, p75, mean}},
  "salary_by_devtype_usd": {devtype: {n, p25, p50, p75, mean}},
  "language_use_pct": {language: {"used_pct": float, "admired_pct": float, "n_used": int}},
  "database_admired_pct": {db: {"admired_pct": float, "n": int}},
  "platform_admired_pct": {platform: {...}},
  "ai_models_admired_pct": {model: {...}},
}
"""

from __future__ import annotations

import csv
import datetime
import json
import logging
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable

# SO Survey rows can have very long multi-select fields (semicolon lists).
# Bump csv field size limit to int max (with platform-safe fallback).
_max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(_max_int)
        break
    except OverflowError:
        _max_int = int(_max_int / 10)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

CSV_PATH = Path("data/so_survey_2025/survey_results_public.csv")
OUT_PATH = Path("data/so_survey_2025_aggregates.json")

# How many top items to keep in each "admired/used" leaderboard.
TOP_N = 50

# Multi-value columns are semicolon-delimited in the SO Survey export.
MULTI_DELIM = ";"

# Salary outlier filter (USD): exclude obvious data-entry errors.
SALARY_MIN = 5_000.0
SALARY_MAX = 5_000_000.0

# YearsCode bucketing.
YEARSCODE_BUCKETS = [
    ("<1", lambda v: v < 1),
    ("1-2", lambda v: 1 <= v < 3),
    ("3-5", lambda v: 3 <= v < 6),
    ("6-10", lambda v: 6 <= v < 11),
    ("11-15", lambda v: 11 <= v < 16),
    ("16-20", lambda v: 16 <= v < 21),
    ("21-30", lambda v: 21 <= v < 31),
    ("31+", lambda v: v >= 31),
]


def _parse_yearscode(raw: str) -> float | None:
    """Convert YearsCode field to a numeric value.

    SO Survey 2025 uses 'Less than 1 year' and 'More than 50 years' as
    sentinel strings.
    """
    if not raw or raw == "NA":
        return None
    s = raw.strip().lower()
    if "less than 1" in s:
        return 0.5
    if "more than 50" in s:
        return 51.0
    try:
        return float(s)
    except ValueError:
        return None


def _bucket_yearscode(v: float) -> str:
    for label, check in YEARSCODE_BUCKETS:
        if check(v):
            return label
    return "unknown"


def _parse_float(raw: str) -> float | None:
    if not raw or raw == "NA":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _split_multi(raw: str) -> list[str]:
    if not raw or raw == "NA":
        return []
    return [s.strip() for s in raw.split(MULTI_DELIM) if s.strip()]


def _summary(values: Iterable[float]) -> dict:
    arr = sorted(v for v in values if v is not None)
    n = len(arr)
    if n == 0:
        return {"n": 0}
    return {
        "n": n,
        "p25": (
            round(statistics.quantiles(arr, n=4)[0], 0) if n >= 4 else round(arr[0], 0)
        ),
        "p50": round(statistics.median(arr), 0),
        "p75": (
            round(statistics.quantiles(arr, n=4)[2], 0) if n >= 4 else round(arr[-1], 0)
        ),
        "mean": round(statistics.mean(arr), 0),
    }


def main() -> int:
    if not CSV_PATH.exists():
        logger.error("CSV not found at %s", CSV_PATH)
        return 1

    logger.info("Reading %s ...", CSV_PATH)

    country_counter: Counter = Counter()
    devtype_counter: Counter = Counter()
    yearscode_buckets: Counter = Counter()
    salary_country: dict[str, list[float]] = defaultdict(list)
    salary_devtype: dict[str, list[float]] = defaultdict(list)
    salary_country_devtype: dict[str, list[float]] = defaultdict(list)
    lang_used: Counter = Counter()
    lang_admired: Counter = Counter()
    db_admired: Counter = Counter()
    platform_admired: Counter = Counter()
    ai_admired: Counter = Counter()
    webframe_admired: Counter = Counter()

    n_total = 0
    n_with_salary = 0
    with CSV_PATH.open("r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            n_total += 1

            country = (row.get("Country") or "").strip()
            devtype = (row.get("DevType") or "").strip()
            if country and country != "NA":
                country_counter[country] += 1
            if devtype and devtype != "NA":
                devtype_counter[devtype] += 1

            yc = _parse_yearscode(row.get("YearsCode") or "")
            if yc is not None:
                yearscode_buckets[_bucket_yearscode(yc)] += 1

            comp_usd = _parse_float(row.get("ConvertedCompYearly") or "")
            if comp_usd is not None and SALARY_MIN <= comp_usd <= SALARY_MAX:
                n_with_salary += 1
                if country and country != "NA":
                    salary_country[country].append(comp_usd)
                if devtype and devtype != "NA":
                    salary_devtype[devtype].append(comp_usd)
                if country and devtype and country != "NA" and devtype != "NA":
                    salary_country_devtype[f"{country}|{devtype}"].append(comp_usd)

            for lang in _split_multi(row.get("LanguageHaveWorkedWith") or ""):
                lang_used[lang] += 1
            for lang in _split_multi(row.get("LanguageAdmired") or ""):
                lang_admired[lang] += 1
            for db in _split_multi(row.get("DatabaseAdmired") or ""):
                db_admired[db] += 1
            for plat in _split_multi(row.get("PlatformAdmired") or ""):
                platform_admired[plat] += 1
            for m in _split_multi(row.get("AIModelsAdmired") or ""):
                ai_admired[m] += 1
            for w in _split_multi(row.get("WebframeAdmired") or ""):
                webframe_admired[w] += 1

            if n_total % 10_000 == 0:
                logger.info("...%d rows processed", n_total)

    logger.info("Done. Total rows=%d, with valid USD salary=%d", n_total, n_with_salary)

    # Build language pct (used_pct uses n_total; admired uses denominators of those who used the language)
    lang_use_pct: dict = {}
    for lang, used in lang_used.most_common(TOP_N):
        admired = lang_admired.get(lang, 0)
        # "% of users who admire it" = admired / used (caps at 100% in practice)
        admired_pct = round(admired / used * 100, 1) if used else 0.0
        lang_use_pct[lang] = {
            "n_used": used,
            "used_pct": round(used / n_total * 100, 1),
            "n_admired": admired,
            "admired_pct": admired_pct,
        }

    db_pct = {
        db: {"n": cnt, "admired_pct": round(cnt / n_total * 100, 1)}
        for db, cnt in db_admired.most_common(TOP_N)
    }
    platform_pct = {
        p: {"n": cnt, "admired_pct": round(cnt / n_total * 100, 1)}
        for p, cnt in platform_admired.most_common(TOP_N)
    }
    ai_pct = {
        m: {"n": cnt, "admired_pct": round(cnt / n_total * 100, 1)}
        for m, cnt in ai_admired.most_common(TOP_N)
    }
    webframe_pct = {
        w: {"n": cnt, "admired_pct": round(cnt / n_total * 100, 1)}
        for w, cnt in webframe_admired.most_common(TOP_N)
    }

    # Salary aggregates: only keep buckets with n >= 5 (statistical relevance)
    MIN_N = 5
    salary_by_country = {
        c: _summary(vals) for c, vals in salary_country.items() if len(vals) >= MIN_N
    }
    salary_by_devtype = {
        d: _summary(vals) for d, vals in salary_devtype.items() if len(vals) >= MIN_N
    }
    salary_by_country_devtype = {
        k: _summary(vals)
        for k, vals in salary_country_devtype.items()
        if len(vals) >= MIN_N
    }

    out = {
        "metadata": {
            "source": "Stack Overflow 2025 Developer Survey",
            "license": "Open Database License (ODbL)",
            "n_respondents": n_total,
            "n_with_valid_usd_salary": n_with_salary,
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "survey_url": "https://survey.stackoverflow.co/2025/",
            "salary_filters_usd": {"min": SALARY_MIN, "max": SALARY_MAX},
            "min_bucket_n_for_salary_aggregates": MIN_N,
        },
        "country_counts": dict(country_counter.most_common(TOP_N * 2)),
        "devtype_counts": dict(devtype_counter.most_common(TOP_N)),
        "yearscode_distribution": dict(yearscode_buckets),
        "salary_by_country_usd": salary_by_country,
        "salary_by_devtype_usd": salary_by_devtype,
        "salary_by_country_devtype_usd": salary_by_country_devtype,
        "language_use": lang_use_pct,
        "database_admired": db_pct,
        "platform_admired": platform_pct,
        "ai_models_admired": ai_pct,
        "webframe_admired": webframe_pct,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)
    size_kb = OUT_PATH.stat().st_size / 1024
    logger.info(
        "Wrote %s (%.1f KB) -- %d countries, %d devtypes, %d country|devtype salary buckets",
        OUT_PATH,
        size_kb,
        len(salary_by_country),
        len(salary_by_devtype),
        len(salary_by_country_devtype),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
