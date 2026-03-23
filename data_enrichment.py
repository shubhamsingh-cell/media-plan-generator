"""Data Enrichment Engine for Nova AI Suite (v2.0).

Third pillar of self-maintaining infrastructure:
- auto_qc.py: "Is the CODE working?"
- data_matrix_monitor.py: "Can products ACCESS data?"
- data_enrichment.py: "Is the DATA fresh?"

Runs as a background daemon thread with smart per-source intervals:
    - 6h:  Recruitment news, live market data (time-sensitive)
    - 12h: Job board pricing, market trends (slower-moving)
    - 7d:  Salary data, compliance updates (stable/slow-changing)

v2.0 upgrades:
    - Writes enriched data to Supabase (upsert) with local JSON fallback
    - LLM-powered summaries for news, salary, and compliance data
    - Smart per-source enrichment intervals (not a single hourly sweep)
    - Additional Firecrawl tasks: job posting volume, density, ad specs,
      competitor analysis, salary scraping, compliance scraping
    - Enrichment state tracked in Supabase (survives deploys)

Data flow:
    1. Hourly tick checks each source against its per-source interval
    2. Stale sources trigger their enrichment function
    3. Results are written to BOTH local JSON AND Supabase (upsert)
    4. LLM generates summaries for applicable data types
    5. State persisted to Supabase enrichment_log + local fallback
    6. Status exposed via GET /api/health/enrichment (admin-protected)

Dependencies: stdlib + existing project modules (api_enrichment, firecrawl_enrichment, llm_router).
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# -- Scheduling ----------------------------------------------------------------
ENRICHMENT_INTERVAL = 3600  # Check every hour
_INITIAL_DELAY = 300  # 5 minutes after startup (let services warm up)
_MAX_LOG_ENTRIES = 100  # Keep last N enrichment log entries

# -- Paths ---------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent / "data"
ENRICHMENT_STATE_FILE = DATA_DIR / "enrichment_state.json"

# -- Supabase config -----------------------------------------------------------
SUPABASE_URL: str = (
    os.environ.get("SUPABASE_URL") or "https://trpynqjatlhatxpzrvgt.supabase.co"
)
SUPABASE_KEY: str = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_ANON_KEY")
    or ""
)
_SUPABASE_ENABLED = bool(SUPABASE_URL and SUPABASE_KEY)
_HTTP_TIMEOUT = 15
_SSL_CTX = ssl.create_default_context()
_BATCH_SIZE = 100

# -- On-conflict columns for each Supabase table --------------------------------
_ON_CONFLICT_MAP: dict[str, str] = {
    "knowledge_base": "category,key",
    "channel_benchmarks": "channel,industry",
    "vendor_profiles": "name",
    "supply_repository": "name",
    "salary_data": "role,location",
    "compliance_rules": "rule_type,jurisdiction",
    "market_trends": "category,title,source",
    "enrichment_log": "source,started_at",
}

# -- Smart freshness thresholds (hours) per source -----------------------------
# Each source is considered stale after this many hours without a refresh.
FRESHNESS_THRESHOLDS: dict[str, int] = {
    "live_market_data": 12,  # Job board pricing -- every 12h
    "firecrawl_news": 6,  # Recruitment news -- every 6h (time-sensitive)
    "bls_salary": 168,  # Weekly (7d) -- BLS salary benchmarks
    "fred_economic": 168,  # Weekly -- FRED economic indicators
    "adzuna_jobs": 168,  # Weekly -- Adzuna job volume data
    "census_demographics": 720,  # Monthly (30d)
    "compliance_updates": 168,  # Weekly (7d) -- regulatory changes
    "market_trends": 12,  # Market trend data -- every 12h
    "firecrawl_salary": 168,  # Firecrawl salary scraping -- weekly
    "job_posting_volume": 12,  # Job posting volumes -- every 12h
    "job_density": 12,  # Job density by location -- every 12h
    "platform_ad_specs": 168,  # Ad specs -- weekly (rarely change)
    "competitor_analysis": 168,  # Competitor careers -- weekly
}

# -- Top roles for salary enrichment -------------------------------------------
_TOP_SALARY_ROLES: list[str] = [
    "Software Engineer",
    "Registered Nurse",
    "Marketing Manager",
    "Sales Representative",
    "Data Analyst",
    "Product Manager",
    "Truck Driver",
    "Warehouse Associate",
    "Customer Service Rep",
    "Accountant",
]

# -- News topics for Firecrawl enrichment --------------------------------------
_NEWS_TOPICS: list[str] = [
    "recruitment advertising",
    "talent acquisition technology",
    "hiring trends",
]

# -- Job boards for pricing enrichment -----------------------------------------
_JOB_BOARDS: list[str] = [
    "indeed",
    "linkedin",
    "ziprecruiter",
    "glassdoor",
    "monster",
]

# -- Top metros for density analysis -------------------------------------------
_TOP_METROS: list[str] = [
    "New York, NY",
    "Los Angeles, CA",
    "Chicago, IL",
    "Houston, TX",
    "Phoenix, AZ",
    "Dallas, TX",
    "San Francisco, CA",
    "Seattle, WA",
    "Atlanta, GA",
    "Boston, MA",
]

# -- Top employers for competitor analysis -------------------------------------
_TOP_EMPLOYERS: list[str] = [
    "google.com",
    "amazon.com",
    "microsoft.com",
    "meta.com",
    "apple.com",
]

# -- Ad platforms for spec scraping --------------------------------------------
_AD_PLATFORMS: list[str] = [
    "facebook",
    "linkedin",
    "tiktok",
]


# ==============================================================================
# SUPABASE WRITE HELPERS
# ==============================================================================


def _build_supabase_headers(*, prefer: str = "") -> dict[str, str]:
    """Build standard Supabase REST API headers.

    Args:
        prefer: Optional Prefer header value (e.g., 'resolution=merge-duplicates').

    Returns:
        Dict of HTTP headers for Supabase PostgREST requests.
    """
    headers: dict[str, str] = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


def _upsert_to_supabase(
    table: str,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert rows into a Supabase table via PostgREST.

    Uses merge-duplicates semantics so the call is idempotent.
    Batches large payloads to avoid request size limits.
    Failures are logged but never raised -- enrichment must not break
    if Supabase is unavailable.

    Args:
        table: Target Supabase table name.
        rows: List of row dicts to upsert.

    Returns:
        Number of rows successfully upserted (0 on any failure).
    """
    if not rows:
        return 0
    if not _SUPABASE_ENABLED:
        logger.debug("Supabase not configured, skipping upsert to %s", table)
        return 0

    base = SUPABASE_URL.rstrip("/")
    on_conflict = _ON_CONFLICT_MAP.get(table) or ""
    url = f"{base}/rest/v1/{table}"
    if on_conflict:
        url += f"?on_conflict={on_conflict}"

    headers = _build_supabase_headers(prefer="resolution=merge-duplicates")
    total_upserted = 0

    for i in range(0, len(rows), _BATCH_SIZE):
        chunk = rows[i : i + _BATCH_SIZE]
        payload = json.dumps(chunk, ensure_ascii=False, default=str).encode("utf-8")

        try:
            req = urllib.request.Request(
                url, data=payload, method="POST", headers=headers
            )
            with urllib.request.urlopen(
                req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
            ) as resp:
                status = resp.getcode()
                if status and status >= 400:
                    body = resp.read().decode("utf-8", errors="replace")[:300]
                    logger.error(f"Supabase HTTP {status} upserting to {table}: {body}")
                else:
                    total_upserted += len(chunk)
        except urllib.error.HTTPError as exc:
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="replace")[:300]
            except OSError:
                pass
            logger.error(
                f"Supabase HTTP {exc.code} upserting to {table}: {error_body}",
                exc_info=True,
            )
        except urllib.error.URLError as exc:
            logger.error(
                f"Supabase URLError upserting to {table}: {exc.reason}",
                exc_info=True,
            )
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error(f"Supabase error upserting to {table}: {exc}", exc_info=True)

    if total_upserted:
        logger.info(f"Supabase upserted {total_upserted}/{len(rows)} rows to {table}")
    return total_upserted


# ==============================================================================
# LLM ENRICHMENT HELPERS
# ==============================================================================


def _generate_llm_summary(
    prompt: str,
    task_type: str = "narrative",
    max_tokens: int = 1024,
) -> str:
    """Generate an AI summary using the LLM router.

    Wraps the LLM call in try/except so enrichment never fails due to
    LLM unavailability.

    Args:
        prompt: The prompt text to send to the LLM.
        task_type: LLM router task type for routing selection.
        max_tokens: Maximum output tokens.

    Returns:
        Generated text string, or empty string on failure.
    """
    try:
        from llm_router import call_llm

        result = call_llm(
            messages=[{"role": "user", "content": prompt}],
            system_prompt=(
                "You are a recruitment advertising market analyst. "
                "Provide concise, actionable insights for recruitment media planners."
            ),
            max_tokens=max_tokens,
            task_type=task_type,
        )
        return (result.get("text") or "").strip()
    except ImportError:
        logger.warning("llm_router not available for LLM enrichment")
        return ""
    except (ValueError, KeyError, TypeError, OSError, RuntimeError) as exc:
        logger.error(f"LLM enrichment call failed: {exc}", exc_info=True)
        return ""


# ==============================================================================
# ENGINE
# ==============================================================================


class DataEnrichmentEngine:
    """Scheduled data enrichment engine with Supabase writes and LLM summaries.

    Checks data freshness hourly and refreshes stale sources automatically.
    Thread-safe: all shared state is protected by a lock.
    """

    def __init__(self) -> None:
        """Initialize the enrichment engine."""
        self._lock = threading.Lock()
        self._state: dict = self._load_state()
        self._running = False
        self._timer: Optional[threading.Timer] = None
        self._enrichment_log: list[dict] = []

    # -- State persistence -----------------------------------------------------

    def _load_state(self) -> dict:
        """Load enrichment state from Supabase, falling back to local disk."""
        # Try Supabase first
        state = self._load_state_from_supabase()
        if state:
            return state
        # Fall back to local file
        try:
            if ENRICHMENT_STATE_FILE.exists():
                raw = ENRICHMENT_STATE_FILE.read_text(encoding="utf-8")
                return json.loads(raw)
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to load enrichment state: %s", e, exc_info=True)
        return {
            "last_runs": {},
            "stats": {"total_enrichments": 0, "total_failures": 0},
        }

    def _load_state_from_supabase(self) -> Optional[dict]:
        """Load enrichment state from the Supabase enrichment_log table.

        Queries the most recent log entry per source to reconstruct the
        last_runs dict.

        Returns:
            Reconstructed state dict, or None if Supabase is unavailable.
        """
        if not _SUPABASE_ENABLED:
            return None
        try:
            base = SUPABASE_URL.rstrip("/")
            url = (
                f"{base}/rest/v1/enrichment_log"
                "?select=source,started_at,success,records"
                "&order=started_at.desc"
                "&limit=50"
            )
            headers = _build_supabase_headers()
            req = urllib.request.Request(url, method="GET", headers=headers)
            with urllib.request.urlopen(
                req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
            ) as resp:
                raw = resp.read().decode("utf-8")
                rows = json.loads(raw)
                if not isinstance(rows, list) or not rows:
                    return None

            last_runs: dict[str, str] = {}
            total_ok = 0
            total_fail = 0
            for row in rows:
                source = row.get("source") or ""
                if source and source not in last_runs:
                    last_runs[source] = row.get("started_at") or ""
                if row.get("success"):
                    total_ok += 1
                else:
                    total_fail += 1

            return {
                "last_runs": last_runs,
                "stats": {
                    "total_enrichments": total_ok,
                    "total_failures": total_fail,
                },
            }
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            json.JSONDecodeError,
            OSError,
        ) as exc:
            logger.debug(f"Could not load state from Supabase: {exc}")
            return None

    def _save_state(self) -> None:
        """Persist enrichment state to local disk (always) and Supabase (best-effort)."""
        # Always write local file as fallback
        try:
            ENRICHMENT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            ENRICHMENT_STATE_FILE.write_text(
                json.dumps(self._state, indent=2, default=str),
                encoding="utf-8",
            )
        except OSError as e:
            logger.error("Failed to save enrichment state: %s", e, exc_info=True)

    def _save_enrichment_log_to_supabase(
        self, source: str, success: bool, records: int
    ) -> None:
        """Write an enrichment log entry to Supabase.

        Args:
            source: Data source key (e.g., 'firecrawl_news').
            success: Whether the enrichment succeeded.
            records: Number of records processed.
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        row = {
            "source": source,
            "started_at": now_iso,
            "success": success,
            "records": records,
            "metadata": {},
        }
        _upsert_to_supabase("enrichment_log", [row])

    # -- Freshness checks ------------------------------------------------------

    def _is_stale(self, source: str) -> bool:
        """Check if a data source needs refreshing.

        Args:
            source: Key from FRESHNESS_THRESHOLDS.

        Returns:
            True if the source has never been refreshed or its last refresh
            exceeds the configured threshold.
        """
        threshold_hours = FRESHNESS_THRESHOLDS.get(source, 24)
        last_run = self._state.get("last_runs", {}).get(source)
        if not last_run:
            return True
        try:
            last_dt = datetime.fromisoformat(last_run)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) - last_dt > timedelta(
                hours=threshold_hours
            )
        except (ValueError, TypeError):
            return True

    def _mark_refreshed(
        self, source: str, success: bool = True, records: int = 0
    ) -> None:
        """Mark a source as refreshed and update stats.

        Args:
            source: Data source key.
            success: Whether the refresh succeeded.
            records: Number of records enriched (for logging).
        """
        with self._lock:
            if "last_runs" not in self._state:
                self._state["last_runs"] = {}
            if "stats" not in self._state:
                self._state["stats"] = {
                    "total_enrichments": 0,
                    "total_failures": 0,
                }

            self._state["last_runs"][source] = datetime.now(timezone.utc).isoformat()

            if success:
                self._state["stats"]["total_enrichments"] = (
                    self._state["stats"].get("total_enrichments") or 0
                ) + 1
            else:
                self._state["stats"]["total_failures"] = (
                    self._state["stats"].get("total_failures") or 0
                ) + 1

            self._save_state()

            # Write log to Supabase (best-effort)
            self._save_enrichment_log_to_supabase(source, success, records)

            self._enrichment_log.append(
                {
                    "source": source,
                    "success": success,
                    "records": records,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            if len(self._enrichment_log) > _MAX_LOG_ENTRIES:
                self._enrichment_log = self._enrichment_log[-_MAX_LOG_ENTRIES:]

    # -- Individual enrichment tasks -------------------------------------------

    def _enrich_live_market_data(self) -> None:
        """Refresh live market data -- job board pricing via Firecrawl.

        Writes to local JSON AND Supabase channel_benchmarks table.
        """
        if not self._is_stale("live_market_data"):
            return
        try:
            from firecrawl_enrichment import scrape_job_board_pricing

            results: dict = {}
            for board in _JOB_BOARDS:
                try:
                    data = scrape_job_board_pricing(board)
                    if data and data.get("cpc_range"):
                        results[board] = data
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Firecrawl pricing scrape failed for %s: %s",
                        board,
                        e,
                        exc_info=True,
                    )

            if results:
                # -- Local JSON write (fallback) --
                live_path = DATA_DIR / "live_market_data.json"
                existing: dict = {}
                try:
                    if live_path.exists():
                        existing = json.loads(live_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    pass

                existing["job_boards"] = {
                    **existing.get("job_boards", {}),
                    **results,
                }
                existing["scraped_at"] = datetime.now(timezone.utc).isoformat()

                live_path.write_text(
                    json.dumps(existing, indent=2, default=str),
                    encoding="utf-8",
                )

                # -- Supabase upsert --
                sb_rows: list[dict[str, Any]] = []
                for channel_name, board_data in results.items():
                    if not isinstance(board_data, dict):
                        continue
                    sb_rows.append(
                        {
                            "channel": channel_name,
                            "industry": "overall",
                            "cpc": board_data.get("avg_cpc_typical"),
                            "cpa": board_data.get("avg_cpa_min"),
                            "pricing_model": (board_data.get("pricing_model") or ""),
                            "metadata": board_data,
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("channel_benchmarks", sb_rows)

                self._mark_refreshed("live_market_data", True, len(results))
                logger.info("Enriched live market data: %d boards", len(results))
            else:
                self._mark_refreshed("live_market_data", False)

        except ImportError:
            logger.warning("firecrawl_enrichment not available for live market data")
            self._mark_refreshed("live_market_data", False)

    def _enrich_firecrawl_news(self) -> None:
        """Refresh recruitment industry news via Firecrawl.

        Writes to local JSON AND Supabase market_trends table.
        Generates LLM summary of key trends.
        """
        if not self._is_stale("firecrawl_news"):
            return
        try:
            from firecrawl_enrichment import fetch_recruitment_news

            all_news: list[dict] = []
            for topic in _NEWS_TOPICS:
                try:
                    news = fetch_recruitment_news(topic)
                    if news:
                        all_news.extend(news)
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "News fetch failed for '%s': %s", topic, e, exc_info=True
                    )

            if all_news:
                now_iso = datetime.now(timezone.utc).isoformat()

                # -- LLM summary --
                ai_summary = ""
                try:
                    titles = [(a.get("title") or "untitled") for a in all_news[:10]]
                    summary_prompt = (
                        f"Summarize these {len(all_news)} recruitment news articles "
                        f"into 3 key market trends affecting recruitment advertising "
                        f"budgets. Article titles: {json.dumps(titles)[:2000]}"
                    )
                    ai_summary = _generate_llm_summary(summary_prompt)
                except (ValueError, TypeError, RuntimeError) as exc:
                    logger.error(f"LLM news summary failed: {exc}", exc_info=True)

                # -- Local JSON write --
                trends_path = DATA_DIR / "market_trends_live.json"
                trends_path.write_text(
                    json.dumps(
                        {
                            "fetched_at": now_iso,
                            "articles": all_news[:20],
                            "ai_market_summary": ai_summary,
                        },
                        indent=2,
                        default=str,
                    ),
                    encoding="utf-8",
                )

                # -- Supabase upsert to market_trends --
                sb_rows: list[dict[str, Any]] = []
                for article in all_news[:20]:
                    sb_rows.append(
                        {
                            "category": "recruitment_news",
                            "title": (article.get("title") or "Untitled")[:500],
                            "source": (
                                article.get("source")
                                or article.get("url")
                                or "firecrawl"
                            )[:500],
                            "url": (article.get("url") or ""),
                            "summary": (
                                article.get("summary")
                                or article.get("description")
                                or ""
                            )[:2000],
                            "scraped_at": now_iso,
                            "metadata": {
                                "topic": article.get("topic") or "",
                                "ai_market_summary": (
                                    ai_summary[:1000] if ai_summary else ""
                                ),
                            },
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("market_trends", sb_rows)

                self._mark_refreshed("firecrawl_news", True, len(all_news))
                logger.info(
                    "Enriched news: %d articles across %d topics%s",
                    len(all_news),
                    len(_NEWS_TOPICS),
                    " (with AI summary)" if ai_summary else "",
                )
            else:
                self._mark_refreshed("firecrawl_news", False)

        except ImportError:
            logger.warning("firecrawl_enrichment not available for news")
            self._mark_refreshed("firecrawl_news", False)

    def _enrich_bls_salary(self) -> None:
        """Refresh BLS salary data for top roles.

        Writes to Supabase salary_data table with LLM-generated insights.
        """
        if not self._is_stale("bls_salary"):
            return
        try:
            from api_enrichment import fetch_salary_data

            data = fetch_salary_data(_TOP_SALARY_ROLES)
            refreshed = len(data) if data else 0

            if refreshed > 0 and isinstance(data, dict):
                now_iso = datetime.now(timezone.utc).isoformat()

                # -- LLM salary insight --
                ai_insight = ""
                try:
                    salary_summary = json.dumps(
                        {k: v for k, v in list(data.items())[:5]},
                        default=str,
                    )[:2000]
                    insight_prompt = (
                        f"Based on these BLS salary data points for top recruitment "
                        f"roles, provide 3 key salary trends and their impact on "
                        f"recruitment advertising budgets: {salary_summary}"
                    )
                    ai_insight = _generate_llm_summary(insight_prompt)
                except (ValueError, TypeError, RuntimeError) as exc:
                    logger.error(f"LLM salary insight failed: {exc}", exc_info=True)

                # -- Supabase upsert to salary_data --
                sb_rows: list[dict[str, Any]] = []
                for role_name, role_data in data.items():
                    if not isinstance(role_data, dict):
                        continue
                    sb_rows.append(
                        {
                            "role": role_name,
                            "location": (role_data.get("location") or "national"),
                            "median_salary": role_data.get("median_salary")
                            or role_data.get("median"),
                            "salary_range_low": role_data.get("p10")
                            or role_data.get("salary_range_low"),
                            "salary_range_high": role_data.get("p90")
                            or role_data.get("salary_range_high"),
                            "source": "BLS",
                            "scraped_at": now_iso,
                            "metadata": {
                                **role_data,
                                "ai_salary_insight": (
                                    ai_insight[:500] if ai_insight else ""
                                ),
                            },
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("salary_data", sb_rows)

            self._mark_refreshed("bls_salary", refreshed > 0, refreshed)
            logger.info(
                "Enriched BLS salary data: %d/%d roles refreshed",
                refreshed,
                len(_TOP_SALARY_ROLES),
            )

        except ImportError:
            logger.warning("api_enrichment not available for BLS salary data")
            self._mark_refreshed("bls_salary", False)
        except (ValueError, KeyError, TypeError, OSError) as e:
            logger.error("BLS salary enrichment failed: %s", e, exc_info=True)
            self._mark_refreshed("bls_salary", False)

    def _enrich_fred_economic(self) -> None:
        """Refresh FRED economic indicators.

        Writes to Supabase knowledge_base table under 'fred_economic' category.
        """
        if not self._is_stale("fred_economic"):
            return
        try:
            from api_enrichment import fetch_fred_indicators

            data = fetch_fred_indicators()
            if data:
                # -- Supabase upsert to knowledge_base --
                now_iso = datetime.now(timezone.utc).isoformat()
                sb_rows: list[dict[str, Any]] = []
                if isinstance(data, dict):
                    for key, value in data.items():
                        sb_rows.append(
                            {
                                "category": "fred_economic",
                                "key": key,
                                "data": (
                                    value
                                    if isinstance(value, dict)
                                    else {"value": value}
                                ),
                            }
                        )
                elif isinstance(data, list):
                    sb_rows.append(
                        {
                            "category": "fred_economic",
                            "key": "indicators",
                            "data": {"indicators": data, "scraped_at": now_iso},
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("knowledge_base", sb_rows)

                self._mark_refreshed("fred_economic", True, len(data))
                logger.info("Enriched FRED economic indicators: %d series", len(data))
            else:
                self._mark_refreshed("fred_economic", False)

        except ImportError:
            logger.warning("api_enrichment.fetch_fred_indicators not available")
            self._mark_refreshed("fred_economic", False)
        except (ValueError, KeyError, TypeError, OSError) as e:
            logger.error("FRED enrichment failed: %s", e, exc_info=True)
            self._mark_refreshed("fred_economic", False)

    def _enrich_adzuna_jobs(self) -> None:
        """Refresh Adzuna job volume data.

        Writes to Supabase knowledge_base table under 'adzuna_jobs' category.
        """
        if not self._is_stale("adzuna_jobs"):
            return
        try:
            from api_enrichment import fetch_adzuna_data

            categories = [
                "engineering",
                "healthcare",
                "sales",
                "marketing",
                "logistics",
            ]
            total_records = 0
            adzuna_results: dict[str, Any] = {}

            for category in categories:
                try:
                    data = fetch_adzuna_data(category)
                    if data:
                        total_records += 1
                        adzuna_results[category] = data
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Adzuna fetch failed for '%s': %s",
                        category,
                        e,
                        exc_info=True,
                    )

            # -- Supabase upsert --
            if adzuna_results:
                now_iso = datetime.now(timezone.utc).isoformat()
                sb_rows: list[dict[str, Any]] = []
                for cat_name, cat_data in adzuna_results.items():
                    sb_rows.append(
                        {
                            "category": "adzuna_jobs",
                            "key": cat_name,
                            "data": (
                                cat_data
                                if isinstance(cat_data, dict)
                                else {"value": cat_data}
                            ),
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("knowledge_base", sb_rows)

            self._mark_refreshed("adzuna_jobs", total_records > 0, total_records)
            logger.info(
                "Enriched Adzuna job data: %d/%d categories",
                total_records,
                len(categories),
            )

        except (ImportError, AttributeError):
            logger.warning("api_enrichment.fetch_adzuna_data not available")
            self._mark_refreshed("adzuna_jobs", False)

    def _enrich_census_demographics(self) -> None:
        """Refresh Census demographic data (monthly).

        Writes to Supabase knowledge_base table under 'census_demographics' category.
        """
        if not self._is_stale("census_demographics"):
            return
        try:
            from api_enrichment import fetch_census_data

            data = fetch_census_data()
            if data:
                # -- Supabase upsert --
                now_iso = datetime.now(timezone.utc).isoformat()
                sb_rows: list[dict[str, Any]] = []
                if isinstance(data, dict):
                    for key, value in data.items():
                        sb_rows.append(
                            {
                                "category": "census_demographics",
                                "key": key,
                                "data": (
                                    value
                                    if isinstance(value, dict)
                                    else {"value": value}
                                ),
                            }
                        )
                elif isinstance(data, list):
                    sb_rows.append(
                        {
                            "category": "census_demographics",
                            "key": "demographics",
                            "data": {"records": data, "scraped_at": now_iso},
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("knowledge_base", sb_rows)

                self._mark_refreshed("census_demographics", True, len(data))
                logger.info("Enriched Census demographics: %d records", len(data))
            else:
                self._mark_refreshed("census_demographics", False)

        except (ImportError, AttributeError):
            logger.warning("api_enrichment.fetch_census_data not available")
            self._mark_refreshed("census_demographics", False)
        except (ValueError, KeyError, TypeError, OSError) as e:
            logger.error("Census enrichment failed: %s", e, exc_info=True)
            self._mark_refreshed("census_demographics", False)

    def _enrich_compliance_updates(self) -> None:
        """Check for compliance/regulatory updates.

        Uses dedicated scrape_compliance_updates() from firecrawl_enrichment.
        Writes to local JSON AND Supabase compliance_rules table.
        Generates LLM impact assessment.
        """
        if not self._is_stale("compliance_updates"):
            return
        try:
            from firecrawl_enrichment import scrape_compliance_updates

            all_updates: list[dict] = []
            try:
                updates = scrape_compliance_updates()
                if updates:
                    all_updates.extend(updates)
            except (ValueError, KeyError, TypeError, OSError) as e:
                logger.error(
                    "Compliance scrape failed: %s",
                    e,
                    exc_info=True,
                )

            # Also fetch via news as fallback
            if len(all_updates) < 5:
                try:
                    from firecrawl_enrichment import fetch_recruitment_news

                    compliance_topics = [
                        "recruitment compliance regulations",
                        "hiring law changes",
                        "employment advertising compliance",
                    ]
                    for topic in compliance_topics:
                        try:
                            news_updates = fetch_recruitment_news(topic)
                            if news_updates:
                                all_updates.extend(news_updates)
                        except (ValueError, KeyError, TypeError, OSError) as e:
                            logger.error(
                                "Compliance news fetch failed for '%s': %s",
                                topic,
                                e,
                                exc_info=True,
                            )
                except ImportError:
                    pass

            if all_updates:
                now_iso = datetime.now(timezone.utc).isoformat()

                # -- LLM compliance impact assessment --
                ai_assessment = ""
                try:
                    update_titles = [
                        (u.get("title") or u.get("summary") or "update")
                        for u in all_updates[:8]
                    ]
                    assessment_prompt = (
                        f"Based on these {len(all_updates)} compliance/regulatory updates, "
                        f"provide a brief impact assessment for recruitment advertisers: "
                        f"what must they change in their job ads? Updates: "
                        f"{json.dumps(update_titles)[:2000]}"
                    )
                    ai_assessment = _generate_llm_summary(
                        assessment_prompt, task_type="research"
                    )
                except (ValueError, TypeError, RuntimeError) as exc:
                    logger.error(
                        f"LLM compliance assessment failed: {exc}", exc_info=True
                    )

                # -- Local JSON write --
                compliance_path = DATA_DIR / "compliance_updates.json"
                compliance_path.write_text(
                    json.dumps(
                        {
                            "fetched_at": now_iso,
                            "updates": all_updates[:15],
                            "ai_compliance_assessment": ai_assessment,
                        },
                        indent=2,
                        default=str,
                    ),
                    encoding="utf-8",
                )

                # -- Supabase upsert to compliance_rules --
                sb_rows: list[dict[str, Any]] = []
                for update in all_updates[:15]:
                    sb_rows.append(
                        {
                            "rule_type": (
                                update.get("rule_type")
                                or update.get("category")
                                or "regulatory_update"
                            ),
                            "jurisdiction": (update.get("jurisdiction") or "federal")[
                                :200
                            ],
                            "title": (update.get("title") or "Compliance Update")[:500],
                            "description": (
                                update.get("summary") or update.get("description") or ""
                            )[:2000],
                            "effective_date": update.get("effective_date"),
                            "source_url": (update.get("url") or ""),
                            "status": "active",
                            "metadata": {
                                "ai_assessment": (
                                    ai_assessment[:500] if ai_assessment else ""
                                ),
                                "scraped_at": now_iso,
                            },
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("compliance_rules", sb_rows)

                self._mark_refreshed("compliance_updates", True, len(all_updates))
                logger.info("Enriched compliance updates: %d items", len(all_updates))
            else:
                self._mark_refreshed("compliance_updates", False)

        except ImportError:
            logger.warning("firecrawl_enrichment not available for compliance")
            self._mark_refreshed("compliance_updates", False)

    # -- NEW Firecrawl enrichment tasks ----------------------------------------

    def _enrich_firecrawl_salary(self) -> None:
        """Scrape salary data via Firecrawl for top roles.

        Writes to Supabase salary_data table.
        """
        if not self._is_stale("firecrawl_salary"):
            return
        try:
            from firecrawl_enrichment import scrape_salary_data

            now_iso = datetime.now(timezone.utc).isoformat()
            total = 0
            sb_rows: list[dict[str, Any]] = []

            for role in _TOP_SALARY_ROLES[:5]:  # Limit to avoid rate limits
                try:
                    data = scrape_salary_data(role)
                    if data and data.get("source") != "error":
                        total += 1
                        sb_rows.append(
                            {
                                "role": role,
                                "location": (data.get("location") or "national"),
                                "median_salary": data.get("median_salary")
                                or data.get("median"),
                                "salary_range_low": data.get("salary_range_low")
                                or data.get("p10"),
                                "salary_range_high": data.get("salary_range_high")
                                or data.get("p90"),
                                "source": "firecrawl",
                                "scraped_at": now_iso,
                                "metadata": data,
                            }
                        )
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Firecrawl salary scrape failed for '%s': %s",
                        role,
                        e,
                        exc_info=True,
                    )

            if sb_rows:
                _upsert_to_supabase("salary_data", sb_rows)

            self._mark_refreshed("firecrawl_salary", total > 0, total)
            logger.info("Enriched Firecrawl salary data: %d roles", total)

        except ImportError:
            logger.warning("firecrawl_enrichment.scrape_salary_data not available")
            self._mark_refreshed("firecrawl_salary", False)

    def _enrich_job_posting_volume(self) -> None:
        """Scrape job posting volumes for popular roles.

        Writes to Supabase knowledge_base table.
        """
        if not self._is_stale("job_posting_volume"):
            return
        try:
            from firecrawl_enrichment import scrape_job_posting_volume

            now_iso = datetime.now(timezone.utc).isoformat()
            total = 0
            volume_data: dict[str, Any] = {}

            for role in _TOP_SALARY_ROLES[:5]:
                try:
                    data = scrape_job_posting_volume(role)
                    if data and data.get("estimated_openings", 0) > 0:
                        total += 1
                        volume_data[role] = data
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Job posting volume scrape failed for '%s': %s",
                        role,
                        e,
                        exc_info=True,
                    )

            if volume_data:
                # -- Local JSON write --
                vol_path = DATA_DIR / "job_posting_volumes.json"
                vol_path.write_text(
                    json.dumps(
                        {"scraped_at": now_iso, "volumes": volume_data},
                        indent=2,
                        default=str,
                    ),
                    encoding="utf-8",
                )

                # -- Supabase upsert --
                sb_rows: list[dict[str, Any]] = []
                for role_name, role_vol in volume_data.items():
                    sb_rows.append(
                        {
                            "category": "job_posting_volume",
                            "key": role_name,
                            "data": role_vol,
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("knowledge_base", sb_rows)

            self._mark_refreshed("job_posting_volume", total > 0, total)
            logger.info("Enriched job posting volumes: %d roles", total)

        except ImportError:
            logger.warning(
                "firecrawl_enrichment.scrape_job_posting_volume not available"
            )
            self._mark_refreshed("job_posting_volume", False)

    def _enrich_job_density(self) -> None:
        """Scrape job density by location for top metros.

        Writes to Supabase knowledge_base table.
        """
        if not self._is_stale("job_density"):
            return
        try:
            from firecrawl_enrichment import scrape_job_density_by_location

            now_iso = datetime.now(timezone.utc).isoformat()
            total = 0
            density_data: dict[str, Any] = {}

            # Use a representative role set
            sample_roles = [
                "Software Engineer",
                "Registered Nurse",
                "Sales Representative",
            ]
            for role in sample_roles:
                try:
                    data = scrape_job_density_by_location(role, _TOP_METROS)
                    if data and data.get("locations"):
                        total += 1
                        density_data[role] = data
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Job density scrape failed for '%s': %s",
                        role,
                        e,
                        exc_info=True,
                    )

            if density_data:
                # -- Local JSON write --
                density_path = DATA_DIR / "job_density_metros.json"
                density_path.write_text(
                    json.dumps(
                        {"scraped_at": now_iso, "density": density_data},
                        indent=2,
                        default=str,
                    ),
                    encoding="utf-8",
                )

                # -- Supabase upsert --
                sb_rows: list[dict[str, Any]] = []
                for role_name, role_density in density_data.items():
                    sb_rows.append(
                        {
                            "category": "job_density",
                            "key": role_name,
                            "data": role_density,
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("knowledge_base", sb_rows)

            self._mark_refreshed("job_density", total > 0, total)
            logger.info("Enriched job density data: %d roles", total)

        except ImportError:
            logger.warning(
                "firecrawl_enrichment.scrape_job_density_by_location not available"
            )
            self._mark_refreshed("job_density", False)

    def _enrich_platform_ad_specs(self) -> None:
        """Scrape ad specifications for major advertising platforms.

        Writes to Supabase knowledge_base table.
        """
        if not self._is_stale("platform_ad_specs"):
            return
        try:
            from firecrawl_enrichment import scrape_platform_ad_specs

            now_iso = datetime.now(timezone.utc).isoformat()
            total = 0
            specs_data: dict[str, Any] = {}

            for platform in _AD_PLATFORMS:
                try:
                    data = scrape_platform_ad_specs(platform)
                    if data and data.get("source") != "fallback_specs":
                        total += 1
                        specs_data[platform] = data
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Ad specs scrape failed for '%s': %s",
                        platform,
                        e,
                        exc_info=True,
                    )

            if specs_data:
                # -- Local JSON write --
                specs_path = DATA_DIR / "platform_ad_specs.json"
                specs_path.write_text(
                    json.dumps(
                        {"scraped_at": now_iso, "platforms": specs_data},
                        indent=2,
                        default=str,
                    ),
                    encoding="utf-8",
                )

                # -- Supabase upsert --
                sb_rows: list[dict[str, Any]] = []
                for platform_name, platform_specs in specs_data.items():
                    sb_rows.append(
                        {
                            "category": "platform_ad_specs",
                            "key": platform_name,
                            "data": platform_specs,
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("knowledge_base", sb_rows)

            self._mark_refreshed("platform_ad_specs", total > 0, total)
            logger.info("Enriched ad specs: %d platforms", total)

        except ImportError:
            logger.warning(
                "firecrawl_enrichment.scrape_platform_ad_specs not available"
            )
            self._mark_refreshed("platform_ad_specs", False)

    def _enrich_competitor_analysis(self) -> None:
        """Analyze careers pages of top employers.

        Writes to Supabase knowledge_base table.
        """
        if not self._is_stale("competitor_analysis"):
            return
        try:
            from firecrawl_enrichment import analyze_competitor_careers

            now_iso = datetime.now(timezone.utc).isoformat()
            total = 0
            competitor_data: dict[str, Any] = {}

            for domain in _TOP_EMPLOYERS:
                try:
                    data = analyze_competitor_careers(domain)
                    if data and data.get("source") != "error":
                        total += 1
                        competitor_data[domain] = data
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Competitor analysis failed for '%s': %s",
                        domain,
                        e,
                        exc_info=True,
                    )

            if competitor_data:
                # -- Local JSON write --
                comp_path = DATA_DIR / "competitor_careers.json"
                comp_path.write_text(
                    json.dumps(
                        {"scraped_at": now_iso, "competitors": competitor_data},
                        indent=2,
                        default=str,
                    ),
                    encoding="utf-8",
                )

                # -- Supabase upsert --
                sb_rows: list[dict[str, Any]] = []
                for comp_domain, comp_info in competitor_data.items():
                    sb_rows.append(
                        {
                            "category": "competitor_careers",
                            "key": comp_domain,
                            "data": comp_info,
                        }
                    )
                if sb_rows:
                    _upsert_to_supabase("knowledge_base", sb_rows)

            self._mark_refreshed("competitor_analysis", total > 0, total)
            logger.info("Enriched competitor analysis: %d employers", total)

        except ImportError:
            logger.warning(
                "firecrawl_enrichment.analyze_competitor_careers not available"
            )
            self._mark_refreshed("competitor_analysis", False)

    # -- Cycle runner ----------------------------------------------------------

    def run_cycle(self) -> dict:
        """Run one enrichment cycle. Check all sources and refresh stale ones.

        Returns:
            Dict with cycle results: checked, refreshed, skipped, failed counts.
        """
        logger.info("Data enrichment cycle starting...")
        cycle_start = time.time()

        tasks: list[tuple[str, callable]] = [
            # Original tasks
            ("live_market_data", self._enrich_live_market_data),
            ("firecrawl_news", self._enrich_firecrawl_news),
            ("bls_salary", self._enrich_bls_salary),
            ("fred_economic", self._enrich_fred_economic),
            ("adzuna_jobs", self._enrich_adzuna_jobs),
            ("census_demographics", self._enrich_census_demographics),
            ("compliance_updates", self._enrich_compliance_updates),
            # NEW Firecrawl tasks
            ("firecrawl_salary", self._enrich_firecrawl_salary),
            ("job_posting_volume", self._enrich_job_posting_volume),
            ("job_density", self._enrich_job_density),
            ("platform_ad_specs", self._enrich_platform_ad_specs),
            ("competitor_analysis", self._enrich_competitor_analysis),
        ]

        results: dict = {
            "checked": 0,
            "refreshed": 0,
            "skipped": 0,
            "failed": 0,
        }

        for source, task_fn in tasks:
            results["checked"] += 1
            if not self._is_stale(source):
                results["skipped"] += 1
                continue
            try:
                task_fn()
                # Task internally calls _mark_refreshed; check last log entry
                if self._enrichment_log and self._enrichment_log[-1].get("success"):
                    results["refreshed"] += 1
                else:
                    results["failed"] += 1
            except (ValueError, KeyError, TypeError, OSError, RuntimeError) as e:
                results["failed"] += 1
                logger.error(
                    "Enrichment task '%s' failed: %s", source, e, exc_info=True
                )

        elapsed = round(time.time() - cycle_start, 2)
        results["elapsed_seconds"] = elapsed
        results["timestamp"] = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Data enrichment cycle complete: %d checked, %d refreshed, "
            "%d skipped, %d failed (%.1fs)",
            results["checked"],
            results["refreshed"],
            results["skipped"],
            results["failed"],
            elapsed,
        )
        return results

    # -- Background loop -------------------------------------------------------

    def start(self) -> None:
        """Start the background enrichment loop.

        First cycle runs after a 5-minute delay to let services warm up.
        Subsequent cycles run every ENRICHMENT_INTERVAL seconds (1 hour).
        """
        if self._running:
            return
        self._running = True
        self._timer = threading.Timer(_INITIAL_DELAY, self._loop)
        self._timer.daemon = True
        self._timer.name = "data-enrichment"
        self._timer.start()
        logger.info(
            "Data enrichment engine started (first run in %d seconds)", _INITIAL_DELAY
        )

    def stop(self) -> None:
        """Stop the background enrichment loop."""
        self._running = False
        if self._timer:
            self._timer.cancel()
            self._timer = None
        logger.info("Data enrichment engine stopped")

    def _loop(self) -> None:
        """Background loop -- run cycle, then schedule next."""
        if not self._running:
            return
        try:
            self.run_cycle()
        except (ValueError, KeyError, TypeError, OSError, RuntimeError) as e:
            logger.error("Enrichment cycle crashed: %s", e, exc_info=True)

        if self._running:
            self._timer = threading.Timer(ENRICHMENT_INTERVAL, self._loop)
            self._timer.daemon = True
            self._timer.name = "data-enrichment"
            self._timer.start()

    # -- Status reporting ------------------------------------------------------

    def get_status(self) -> dict:
        """Get current enrichment status for health endpoint.

        Returns:
            Dict with running state, persisted state, recent log entries,
            per-source freshness details, and Supabase connectivity.
        """
        with self._lock:
            freshness: dict = {}
            for source, hours in FRESHNESS_THRESHOLDS.items():
                last_run = self._state.get("last_runs", {}).get(source)
                freshness[source] = {
                    "stale": self._is_stale(source),
                    "threshold_hours": hours,
                    "last_run": last_run,
                }

            return {
                "running": self._running,
                "supabase_enabled": _SUPABASE_ENABLED,
                "state": {
                    "last_runs": dict(self._state.get("last_runs", {})),
                    "stats": dict(self._state.get("stats", {})),
                },
                "recent_log": list(self._enrichment_log[-10:]),
                "freshness": freshness,
            }


# ==============================================================================
# MODULE-LEVEL SINGLETON
# ==============================================================================

_engine: Optional[DataEnrichmentEngine] = None
_engine_lock = threading.Lock()


def get_engine() -> DataEnrichmentEngine:
    """Get or create the singleton enrichment engine.

    Returns:
        The DataEnrichmentEngine singleton instance.
    """
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = DataEnrichmentEngine()
    return _engine


def start_enrichment() -> None:
    """Start the enrichment engine (convenience wrapper)."""
    get_engine().start()


def get_enrichment_status() -> dict:
    """Get enrichment status for health endpoints.

    Returns:
        Dict with engine status, freshness, and recent activity.
    """
    return get_engine().get_status()
