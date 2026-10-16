"""Trust features for generated plans: "data as of" freshness and the
review-before-sending checklist.

Both the workbook (excel_v2) and the deck (ppt_generator) import this module,
so the two deliverables state the SAME dates and the SAME checklist.

Nothing here invents a fact:

* A source's date is read from metadata that already sits in the data file
  (``last_updated`` / ``_last_verified`` / ``_refreshed_iso`` / ...). A file
  with no readable date is reported as ``date not recorded``.
* A checklist item exists only when the plan carries the flag that triggers it.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.join(_BASE_DIR, "data")

NOT_RECORDED = "date not recorded"

# Source families: (label, data files). Only files the plan actually reads.
# Derived from the files' own metadata -- nothing here carries a date.
SOURCE_FAMILIES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("Supply repository", ("joveo_global_supply_repository.json",)),
    (
        "Publisher network & country boards",
        ("joveo_publishers.json", "global_supply.json"),
    ),
    (
        "Joveo benchmarks (CPC / CPA)",
        (
            "joveo_2026_benchmarks.json",
            "joveo_cpa_benchmarks_2026.json",
            "publisher_benchmarks_2026.json",
        ),
    ),
    (
        "Recruitment & ad benchmarks",
        (
            "recruitment_benchmarks_2026_deep.json",
            "ad_benchmarks_recruitment_2026.json",
        ),
    ),
    ("Salary benchmarks", ("salary_benchmarks_detailed_2026.json",)),
    (
        "Seasonal hiring patterns",
        ("seasonal_hiring_trends.json", "subvertical_seasonal_overrides.json"),
    ),
    (
        "International benchmarks",
        ("international_benchmarks_2026.json", "intl_role_benchmarks_v1.json"),
    ),
    ("Labour-market outlook", ("labor_market_outlook_2026.json",)),
    (
        "Scheduled refresh feeds (Adzuna / FRED / Google Trends)",
        ("adzuna_benchmarks.json", "fred_indicators.json", "google_trends_seed.json"),
    ),
)

# Metadata containers, then keys, in priority order. "Last refreshed" beats
# "researched" (the original research date) when a file carries both.
_CONTAINER_KEYS = ("_metadata", "metadata", "_meta", "meta")
_DATE_KEYS = (
    "last_updated",
    "_last_verified",
    "last_verified",
    "as_of",
    "_refreshed_iso",
    "fetched_at",
    "generated_at",
    "generated",
    "generated_date",
    "compiled_date",
    "researched_date",
)

_ISO_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
_QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")


def _coerce_date(value: Any) -> Optional[str]:
    """``YYYY-MM-DD`` (or ``YYYY-Qn``) from a metadata value, else None."""
    if isinstance(value, str):
        text = value.strip()
        match = _ISO_DATE_RE.match(text)
        if match:
            try:
                datetime.date.fromisoformat(match.group(1))
            except ValueError:
                return None
            return match.group(1)
        if _QUARTER_RE.match(text):
            return text
    return None


def read_as_of(path: str) -> Optional[str]:
    """The last-refreshed date recorded inside one data file, or None."""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
    except (OSError, ValueError) as exc:
        logger.warning("plan_trust: cannot read %s: %s", path, exc)
        return None
    if not isinstance(doc, dict):
        return None
    containers: List[Dict[str, Any]] = [
        doc[key] for key in _CONTAINER_KEYS if isinstance(doc.get(key), dict)
    ]
    containers.append(doc)
    for container in containers:
        for key in _DATE_KEYS:
            found = _coerce_date(container.get(key))
            if found:
                return found
    # The refresh pipeline writes an epoch beside the ISO stamp; use it only
    # when no ISO date exists.
    stamp = doc.get("_refreshed_at")
    if isinstance(stamp, (int, float)) and not isinstance(stamp, bool) and stamp > 0:
        try:
            return datetime.datetime.fromtimestamp(
                stamp, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%d")
        except (OverflowError, OSError, ValueError):
            return None
    return None


def _family_row(label: str, files: Sequence[str], data_dir: str) -> Dict[str, Any]:
    dated: List[str] = []
    undated = 0
    for name in files:
        found = read_as_of(os.path.join(data_dir, name))
        if found:
            dated.append(found)
        else:
            undated += 1
    if not dated:
        text = NOT_RECORDED
        oldest = None
    else:
        lo, hi = min(dated), max(dated)
        text = lo if lo == hi else f"{lo} to {hi}"
        if undated:
            text += f" ({undated} of {len(files)} files: {NOT_RECORDED})"
        oldest = lo
    return {
        "label": label,
        "as_of": text,
        "oldest": oldest,
        "files": list(files),
    }


def deploy_version() -> Tuple[str, str]:
    """(version, git sha) from the same source as /api/deploy/ready: the app
    module's ``_DEPLOY_VERSION`` / ``_DEPLOY_GIT_COMMIT``, else the Render
    commit env var, else ``local``."""
    app_mod = sys.modules.get("app")
    version = getattr(app_mod, "_DEPLOY_VERSION", None)
    commit = getattr(app_mod, "_DEPLOY_GIT_COMMIT", None)
    if not commit:
        commit = os.environ.get("RENDER_GIT_COMMIT") or "local"
    if not version:
        version = f"4.0.0-{str(commit)[:8]}"
    return str(version), str(commit)


def _live_pull_line(data: Any, generated_label: str) -> str:
    enriched = data.get("_enriched") if isinstance(data, dict) else None
    summary = enriched.get("enrichment_summary") if isinstance(enriched, dict) else None
    if not isinstance(summary, dict) or not summary:
        return "Live API pulls: not recorded for this plan"
    ok = summary.get("apis_succeeded") or []
    n_ok = len(ok) if isinstance(ok, (list, tuple, set)) else 0
    if not n_ok:
        return "Live API pulls: none returned data for this plan"
    noun = "source" if n_ok == 1 else "sources"
    return f"Live API pulls: {n_ok} {noun} returned data, pulled {generated_label}"


def data_asof(
    data: Any = None,
    now: Optional[datetime.datetime] = None,
    data_dir: Optional[str] = None,
    families: Optional[Sequence[Tuple[str, Sequence[str]]]] = None,
) -> Dict[str, Any]:
    """Freshness record for one generated plan.

    ``families``/``data_dir`` default to the repo's; tests inject their own.
    """
    moment = now or datetime.datetime.now(datetime.timezone.utc)
    generated_label = moment.strftime("%Y-%m-%d %H:%M UTC")
    version, commit = deploy_version()
    rows = [
        _family_row(label, files, data_dir or DEFAULT_DATA_DIR)
        for label, files in (families if families is not None else SOURCE_FAMILIES)
    ]
    live = _live_pull_line(data, generated_label)
    return {
        "generated_at": moment.isoformat(timespec="seconds"),
        "generated_label": generated_label,
        "app_version": version,
        "git_sha": commit,
        "families": rows,
        "live_line": live,
        "headline": headline(rows),
    }


def headline(rows: Sequence[Dict[str, Any]]) -> str:
    """One short line for a slide footer / checklist: the supply and
    benchmark dates (oldest file in each family, so it never overstates)."""
    by_label = {r["label"]: r for r in rows}
    supply = by_label.get(SOURCE_FAMILIES[0][0])
    bench = by_label.get(SOURCE_FAMILIES[2][0])

    def _d(row: Optional[Dict[str, Any]]) -> str:
        return (row or {}).get("oldest") or NOT_RECORDED

    return f"Data as of: supply {_d(supply)} | benchmarks {_d(bench)}"


def provenance_stamp(info: Dict[str, Any]) -> str:
    """'Generated 2026-10-17 14:03 UTC | Nova 4.0.0-abcd1234' for file
    properties and footers."""
    return f"Generated {info['generated_label']} | Nova {info['app_version']}"


# ---------------------------------------------------------------------------
# Review-before-sending checklist
# ---------------------------------------------------------------------------

# Budget reality-check tiers worth a second look before the plan goes out.
_FLAGGED_TIERS = ("impossible", "severely_underfunded", "underfunded", "tight")

_DAGGER_TOKEN_RE = re.compile(r"\s*†\s*$")


def count_unverified_boards(texts: Iterable[str]) -> int:
    """Distinct board names carrying a trailing † in the given strings.

    Boards render as ``Name †`` inside comma-separated lists; a lone legend
    line ("† relationship unverified -- ...") has no name before the mark and
    is not counted."""
    seen = set()
    for text in texts:
        if not isinstance(text, str) or "†" not in text:
            continue
        for part in re.split(r"[,;\n|]", text):
            if _DAGGER_TOKEN_RE.search(part):
                name = _DAGGER_TOKEN_RE.sub("", part).strip(" -•\t")
                if name:
                    seen.add(name.lower())
    return len(seen)


def _non_us_markets(data: Any) -> List[str]:
    try:
        import publisher_filter

        return [
            c
            for c in publisher_filter.plan_countries(data)
            if c != "United States"
        ]
    except (ImportError, AttributeError, TypeError, ValueError):
        logger.warning("plan_trust: market lookup failed", exc_info=True)
        return []


def _markets_without_benchmarks(data: Any) -> List[str]:
    """Plan markets with no local benchmark: no board data in the dataset, or
    the plan's cost-per-hire claim was suppressed for lack of a local figure."""
    markets: List[str] = []
    try:
        import publisher_filter

        markets.extend(publisher_filter.plan_market_board_gaps(data))
    except (ImportError, AttributeError, TypeError, ValueError):
        logger.warning("plan_trust: board-gap lookup failed", exc_info=True)
    alloc = data.get("_budget_allocation") if isinstance(data, dict) else None
    alloc = alloc if isinstance(alloc, dict) else {}
    meta = alloc.get("metadata") if isinstance(alloc.get("metadata"), dict) else {}
    cph = meta.get("industry_cph") if isinstance(meta.get("industry_cph"), dict) else {}
    suff = alloc.get("sufficiency") if isinstance(alloc.get("sufficiency"), dict) else {}
    check = suff.get("budget_reality_check")
    check = check if isinstance(check, dict) else {}
    if cph.get("claim_suppressed") or check.get("feasibility_tier") == "not_assessed":
        for market in _non_us_markets(data):
            if market not in markets:
                markets.append(market)
    return markets


def review_items(
    data: Any,
    unverified_boards: int = 0,
    asof: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Plain-language checklist for THIS plan, in reading order. An item is
    present only when the plan's own flag is true; the last three are
    standing items that apply to every plan."""
    data = data if isinstance(data, dict) else {}
    items: List[str] = []

    if not data.get("historical_performance"):
        items.append(
            "Projections are ranges based on industry benchmarks — no client "
            "hire history was provided. Present hires and cost per hire as "
            "estimates, not commitments."
        )

    if unverified_boards > 0:
        noun = "board is" if unverified_boards == 1 else "boards are"
        items.append(
            f"{unverified_boards} {noun} marked † relationship unverified — "
            "confirm with supply before quoting."
        )

    alloc = data.get("_budget_allocation")
    alloc = alloc if isinstance(alloc, dict) else {}
    suff = alloc.get("sufficiency") if isinstance(alloc.get("sufficiency"), dict) else {}
    check = suff.get("budget_reality_check")
    if isinstance(check, dict) and check.get("feasibility_tier") in _FLAGGED_TIERS:
        label = str(check.get("feasibility_label") or check["feasibility_tier"])
        items.append(
            f"Budget reality check flagged: {label.title()}. Agree the target "
            "or the budget with the client before this goes out."
        )

    markets = _markets_without_benchmarks(data)
    if markets:
        items.append(
            "Markets without local benchmarks: "
            + ", ".join(markets)
            + ". Figures there are indicative — check local boards and costs "
            "with supply."
        )

    try:
        import publisher_filter

        excluded = publisher_filter.user_excluded(data)
    except (ImportError, AttributeError, TypeError, ValueError):
        logger.warning("plan_trust: exclusion lookup failed", exc_info=True)
        excluded = []
    if excluded:
        # "Excluded at client request: ..." is the one phrase the delivery
        # gates and board sweep allow to name an excluded board.
        items.append(
            "Client exclusions applied. "
            + publisher_filter.disclosure_line(data)
            + ". Confirm the list is complete before sending."
        )

    items.append(
        "Check the client name, markets, roles, budget and timeline against "
        "the brief you were given."
    )
    items.append(
        "Confirm the channel mix and budget split with supply before "
        "committing spend."
    )
    info = asof if asof is not None else data_asof(data)
    items.append(
        f"{info['headline']}. Sources without a recorded date say "
        f"'{NOT_RECORDED}' — check the age is acceptable for this client."
    )
    items.append(
        "This plan was generated by AI. Read every sheet or slide and correct "
        "anything you cannot stand behind — you are responsible for what you "
        "send."
    )
    return items


def checklist_text(items: Sequence[str]) -> str:
    """Numbered plain-text rendering (slide speaker notes)."""
    lines = ["REVIEW BEFORE SENDING"]
    lines.extend(f"{i}. {item}" for i, item in enumerate(items, 1))
    return "\n".join(lines)
