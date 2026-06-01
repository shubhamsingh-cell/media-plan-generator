"""Shared builder for the cited 2026 market-data block used across plan output.

Single source of truth so EVERY media-plan output tier can inject the same
cited 2026 content -- international local salary, cited industry-report metrics,
and an attributed TA-leader quote:

  * Google Slides deck  (joveo_slides_template.py)
  * python-pptx fallback (ppt_generator.py)  <- previously had NONE of this
  * Excel workbook       (excel_v2.py)        <- optional

Before S82 this content lived only inside ``joveo_slides_template.py``, so a
plan rendered via the python-pptx fallback (when Google Slides credentials are
absent or the API errors) shipped with no cited 2026 data at all. This module
lifts the formatting into one reusable, framework-agnostic helper that returns
plain text lines, so the fallback and Excel can render the identical content.

Read-only over the three lookup modules. Never raises -- returns ``[]`` / ``""``
on any miss so callers can splice the result unconditionally.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Defensive imports: output generation must succeed even if a lookup module is
# unavailable in a given deployment. Each shim mirrors the real signature.
try:
    from intl_benchmark_lookup import get_local_salary_summary
except ImportError:  # pragma: no cover -- modules ship in repo

    def get_local_salary_summary(
        industry: Optional[str], country: Optional[str]
    ) -> Optional[dict]:
        return None


try:
    from industry_reports_lookup import get_cited_metrics_for_country
except ImportError:  # pragma: no cover

    def get_cited_metrics_for_country(country: Optional[str], limit: int = 2) -> list:
        return []


try:
    from ta_leaders_lookup import get_top_quotes
except ImportError:  # pragma: no cover

    def get_top_quotes(
        topic_filter: Any = None, limit: int = 3, min_relevance: int = 8
    ) -> list:
        return []


def _format_metric_value(v: Any) -> str:
    """Format a metric value: comma-separated ints, trimmed floats, avoid 1e+06."""
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        if abs(v) >= 1:
            return f"{v:,.2f}".rstrip("0").rstrip(".")
        return f"{v:.3f}".rstrip("0").rstrip(".")
    return str(v) if v is not None else ""


def build_local_salary_line(
    industry: Optional[str], primary_country: Optional[str]
) -> str:
    """Return a single 'Local salary benchmark: ...' line, or '' on miss."""
    try:
        sal = get_local_salary_summary(industry, primary_country)
        if sal and sal.get("local_display"):
            line = f"Local salary benchmark: {sal['local_display']} ({sal['currency']})"
            usd_eq = sal.get("usd_display")
            if usd_eq:
                line += f" [~{usd_eq} USD]"
            return line
    except Exception:  # pragma: no cover -- never break output
        logger.error("local salary enrichment failed", exc_info=True)
    return ""


def build_cited_metrics_lines(primary_country: Optional[str], limit: int = 2) -> list:
    """Return formatted cited-metric lines like '- Metric: 12,345 unit (Pub, 2026)'."""
    out: list = []
    try:
        cited = get_cited_metrics_for_country(primary_country, limit=limit)
        for cm in cited or []:
            v_s = _format_metric_value(cm.get("value"))
            unit = cm.get("unit") or ""
            sep = "" if unit.strip().startswith("%") else " "
            metric_name = (cm.get("metric") or "")[:55]
            publisher = cm.get("publisher") or ""
            if "(" in publisher:
                publisher = publisher.split("(", 1)[0].strip()
            year = cm.get("year") or ""
            out.append(f"{metric_name}: {v_s}{sep}{unit} ({publisher}, {year})")
    except Exception:  # pragma: no cover
        logger.error("F2 cited-metrics enrichment failed", exc_info=True)
    return out


def build_leader_quote_line(limit: int = 1) -> str:
    """Return one attributed TA-leader quote line, or '' on miss."""
    try:
        quotes = get_top_quotes(limit=limit)
        if quotes:
            q = quotes[0]
            raw = (q.get("quote") or "").strip()
            if not raw:
                return ""
            excerpt = raw if len(raw) <= 140 else raw[:140].rsplit(" ", 1)[0] + "..."
            attribution = q.get("name") or ""
            title = (q.get("title") or "").split(",")[0]
            if title:
                attribution = f"{attribution}, {title}"
            if excerpt and attribution:
                return f'"{excerpt}" -- {attribution}'
    except Exception:  # pragma: no cover
        logger.error("F3 leader-quote enrichment failed", exc_info=True)
    return ""


def build_cited_2026_block(data: dict) -> dict:
    """Build the full cited-2026 block from a plan ``data`` dict.

    Returns a dict with three keys (any may be empty -- callers render only the
    non-empty parts):
        {
          "salary_line":   str,        # local salary benchmark, or ""
          "metric_lines":  list[str],  # cited 2026 metrics, or []
          "quote_line":    str,        # attributed TA-leader quote, or ""
        }
    """
    locations = data.get("locations") or []
    industry = data.get("industry_label") or data.get("industry")
    primary_country = (
        locations[0] if isinstance(locations, list) and locations else None
    )
    return {
        "salary_line": build_local_salary_line(industry, primary_country),
        "metric_lines": build_cited_metrics_lines(primary_country, limit=2),
        "quote_line": build_leader_quote_line(limit=1),
    }


def has_cited_content(block: dict) -> bool:
    """True if the block carries any renderable cited content."""
    if not isinstance(block, dict):
        return False
    return bool(
        block.get("salary_line") or block.get("metric_lines") or block.get("quote_line")
    )
