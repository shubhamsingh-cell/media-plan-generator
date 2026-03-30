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

try:
    from alert_manager import send_alert
except ImportError:
    send_alert = lambda *a, **kw: False

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

# -- Supabase auth circuit breaker (per-table, 5-min cooldown) -----------------
# After a 401 Unauthorized on a specific table, only block upserts to THAT table
# for 5 minutes (not all tables for 1 hour). This prevents one bad table from
# killing all 13 enrichment sources.
_SUPABASE_AUTH_COOLDOWN = 300  # 5 minutes (was 3600 = 1 hour)
_supabase_table_fail_times: dict[str, float] = {}  # per-table circuit breakers
_supabase_auth_lock = threading.Lock()
# Legacy global fallback (only used if ALL tables fail within 60s)
_supabase_global_fail_time: float = 0.0
_SUPABASE_GLOBAL_COOLDOWN = 600  # 10 min global cooldown if ALL tables fail

# -- On-conflict columns for each Supabase table --------------------------------
_ON_CONFLICT_MAP: dict[str, str] = {
    "knowledge_base": "category,key",
    "channel_benchmarks": "channel,industry",
    "vendor_profiles": "name",
    "supply_repository": "name",
    "salary_data": "role,location",
    "compliance_rules": "rule_type,jurisdiction",
    "market_trends": "category,title,source",
    "enrichment_log": "source,action",
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
    "benchmark_drift_check": 2160,  # Quarterly (90d) -- compare live CPC/CPA vs stored + file staleness
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
# RETRY HELPER
# ==============================================================================


_RETRYABLE_EXCEPTIONS = (urllib.error.URLError, OSError, TimeoutError, ValueError)


def _retry_with_backoff(
    fn: callable,
    max_retries: int = 3,
    base_delay: float = 2.0,
) -> Any:
    """Execute a callable with exponential backoff on transient failures.

    Retries on network-related exceptions (URLError, OSError, TimeoutError)
    and ValueError. Uses exponential backoff: base_delay * 2^attempt
    (e.g., 2s, 4s, 8s for base_delay=2.0).

    Args:
        fn: Zero-argument callable to execute.
        max_retries: Maximum number of retry attempts (default 3).
        base_delay: Base delay in seconds before first retry (default 2.0).

    Returns:
        The return value of fn on success, or None after all retries exhausted.
    """
    import random as _random_mod

    last_exc: BaseException | None = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except _RETRYABLE_EXCEPTIONS as exc:
            last_exc = exc
            if attempt < max_retries:
                # S27: Add jitter to prevent thundering herd on retries
                delay = base_delay * (2**attempt) + _random_mod.uniform(0, 1.0)
                logger.warning(
                    f"Retry {attempt + 1}/{max_retries} after "
                    f"{delay:.1f}s for {fn.__name__ if hasattr(fn, '__name__') else 'callable'}: {exc}"
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"All {max_retries} retries exhausted for "
                    f"{fn.__name__ if hasattr(fn, '__name__') else 'callable'}: {last_exc}",
                    exc_info=True,
                )
                return None
    return None  # pragma: no cover


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

    # Circuit breaker: per-table 5-min cooldown after 401 Unauthorized
    global _supabase_global_fail_time
    with _supabase_auth_lock:
        # Check global circuit breaker (all tables failed within 60s)
        if _supabase_global_fail_time:
            elapsed = time.monotonic() - _supabase_global_fail_time
            if elapsed < _SUPABASE_GLOBAL_COOLDOWN:
                remaining = int(_SUPABASE_GLOBAL_COOLDOWN - elapsed)
                logger.debug(
                    f"Supabase GLOBAL circuit breaker open -- skipping {table} "
                    f"({remaining}s remaining)"
                )
                return 0
            logger.info("Supabase global circuit breaker reset -- retrying")
            _supabase_global_fail_time = 0.0
            _supabase_table_fail_times.clear()

        # Check per-table circuit breaker
        table_fail_time = _supabase_table_fail_times.get(table, 0.0)
        if table_fail_time:
            elapsed = time.monotonic() - table_fail_time
            if elapsed < _SUPABASE_AUTH_COOLDOWN:
                remaining = int(_SUPABASE_AUTH_COOLDOWN - elapsed)
                logger.debug(
                    f"Supabase circuit breaker open for '{table}' "
                    f"({remaining}s remaining) -- other tables unaffected"
                )
                return 0
            # Per-table cooldown expired, reset and retry this table
            logger.info(f"Supabase circuit breaker reset for '{table}' -- retrying")
            del _supabase_table_fail_times[table]

    base = SUPABASE_URL.rstrip("/")
    on_conflict = _ON_CONFLICT_MAP.get(table) or ""
    url = f"{base}/rest/v1/{table}"
    if on_conflict:
        # URL-encode the on_conflict parameter to handle commas correctly
        encoded_conflict = urllib.parse.quote(on_conflict, safe="")
        url += f"?on_conflict={encoded_conflict}"

    headers = _build_supabase_headers(prefer="resolution=merge-duplicates")
    total_upserted = 0

    for i in range(0, len(rows), _BATCH_SIZE):
        chunk = rows[i : i + _BATCH_SIZE]
        payload = json.dumps(chunk, ensure_ascii=False, default=str).encode("utf-8")

        def _do_upsert_batch(
            _payload: bytes = payload,
            _url: str = url,
            _headers: dict = headers,
        ) -> int:
            """Execute a single Supabase upsert batch with urlopen."""
            req = urllib.request.Request(
                _url, data=_payload, method="POST", headers=_headers
            )
            with urllib.request.urlopen(
                req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
            ) as resp:
                status = resp.getcode()
                if status and status >= 400:
                    body = resp.read().decode("utf-8", errors="replace")[:300]
                    raise ValueError(
                        f"Supabase HTTP {status} upserting to {table}: {body}"
                    )
                return len(chunk)

        try:
            result = _retry_with_backoff(_do_upsert_batch)
            if result is not None:
                total_upserted += result
        except urllib.error.HTTPError as exc:
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="replace")[:300]
            except OSError:
                pass
            if exc.code == 401:
                # Trip PER-TABLE circuit breaker (not global)
                now = time.monotonic()
                with _supabase_auth_lock:
                    _supabase_table_fail_times[table] = now
                    # If 3+ tables failed within 60s, trip the global breaker
                    recent_fails = sum(
                        1 for t in _supabase_table_fail_times.values() if now - t < 60
                    )
                    if recent_fails >= 3:
                        _supabase_global_fail_time = now
                        logger.error(
                            f"Supabase 401 on {recent_fails} tables within 60s -- "
                            f"GLOBAL circuit breaker tripped for "
                            f"{_SUPABASE_GLOBAL_COOLDOWN}s. "
                            f"Check SUPABASE_SERVICE_ROLE_KEY.",
                        )
                logger.error(
                    f"Supabase 401 Unauthorized upserting to '{table}' -- "
                    f"per-table circuit breaker tripped for {_SUPABASE_AUTH_COOLDOWN}s. "
                    f"Other tables unaffected. Body: {error_body}",
                )
                try:
                    send_alert(
                        f"Supabase 401 on '{table}'",
                        f"401 Unauthorized upserting to '{table}'. "
                        f"This table blocked for {_SUPABASE_AUTH_COOLDOWN // 60} min. "
                        f"Other enrichment sources continue normally. "
                        f"Check SUPABASE_SERVICE_ROLE_KEY on Render.",
                        severity="critical",
                    )
                except Exception:
                    pass
                break
            logger.error(
                f"Supabase HTTP {exc.code} upserting to {table}: {error_body}",
                exc_info=True,
            )
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
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


# ==============================================================================
# BENCHMARK FILE FRESHNESS CHECK
# ==============================================================================

_BENCHMARK_FILES: list[str] = [
    "google_ads_2025_benchmarks.json",
    "joveo_2026_benchmarks.json",
    "external_benchmarks_2025.json",
    "recruitment_benchmarks_deep.json",
]

_BENCHMARK_STALE_THRESHOLD_DAYS: int = 90


def check_benchmark_freshness(data_dir: str = "") -> dict[str, dict]:
    """Check age of benchmark data files and flag stale ones.

    Scans the data directory for known benchmark files, computes their age
    from the filesystem modification time, and flags any that exceed the
    90-day staleness threshold.

    Args:
        data_dir: Path to the data directory. Defaults to the module-level DATA_DIR.

    Returns:
        Dict keyed by filename with sub-dicts containing:
            - last_modified (str): ISO-format mtime
            - age_days (int): Days since last modification
            - stale (bool): True if age_days > 90
        Missing files get ``{"missing": True, "stale": True}``.
    """
    base_dir = Path(data_dir) if data_dir else DATA_DIR
    results: dict[str, dict] = {}

    for fname in _BENCHMARK_FILES:
        fpath = base_dir / fname
        try:
            if fpath.exists():
                mtime = datetime.fromtimestamp(fpath.stat().st_mtime, tz=timezone.utc)
                age_days = (datetime.now(timezone.utc) - mtime).days
                stale = age_days > _BENCHMARK_STALE_THRESHOLD_DAYS
                results[fname] = {
                    "last_modified": mtime.isoformat(),
                    "age_days": age_days,
                    "stale": stale,
                }
                if stale:
                    logger.warning(
                        "Benchmark file %s is %d days old (>%d day threshold)",
                        fname,
                        age_days,
                        _BENCHMARK_STALE_THRESHOLD_DAYS,
                    )
            else:
                results[fname] = {"missing": True, "stale": True}
                logger.warning("Benchmark file %s is missing from %s", fname, base_dir)
        except OSError as exc:
            logger.error(
                "Error checking benchmark file %s: %s", fname, exc, exc_info=True
            )
            results[fname] = {"error": str(exc), "stale": True}

    return results


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

        base = SUPABASE_URL.rstrip("/")
        # Encode SELECT parameter to prevent HTTP 400 errors (commas must be %2C)
        select_param = urllib.parse.quote(
            "source,records_affected,details,created_at", safe=""
        )
        url = (
            f"{base}/rest/v1/enrichment_log"
            f"?select={select_param}"
            "&order=created_at.desc"
            "&limit=50"
        )
        headers = _build_supabase_headers()

        def _fetch_enrichment_log() -> list:
            """Fetch enrichment log rows from Supabase."""
            req = urllib.request.Request(url, method="GET", headers=headers)
            with urllib.request.urlopen(
                req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
            ) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)

        try:
            rows = _retry_with_backoff(_fetch_enrichment_log)
            if not isinstance(rows, list) or not rows:
                return None

            last_runs: dict[str, str] = {}
            total_ok = 0
            total_fail = 0
            for row in rows:
                source = row.get("source") or ""
                if source and source not in last_runs:
                    last_runs[source] = row.get("created_at") or ""
                # Parse success from details JSONB
                details = row.get("details")
                if isinstance(details, str):
                    try:
                        details = json.loads(details)
                    except (json.JSONDecodeError, TypeError):
                        details = {}
                if isinstance(details, dict) and details.get("success"):
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
            json.JSONDecodeError,
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
        row = {
            "table_name": "data_enrichment",
            "action": "refresh",
            "source": source,
            "records_affected": records,
            "details": json.dumps({"success": success}),
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

            # Append to in-memory log so run_cycle can track results
            self._enrichment_log.append(
                {
                    "source": source,
                    "success": success,
                    "records": records,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            # Cap log size to prevent memory growth
            if len(self._enrichment_log) > 200:
                self._enrichment_log = self._enrichment_log[-100:]

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
        """Refresh job volume data by category.

        Fallback chain:
          1. Adzuna API (primary)
          2. Jooble API (free, already connected)
          3. RemoteOK API (free, already connected)

        Writes to Supabase knowledge_base table under 'adzuna_jobs' category.
        """
        if not self._is_stale("adzuna_jobs"):
            return

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
            data = None

            # Fallback 1: Adzuna API
            try:
                from api_enrichment import fetch_adzuna_data

                data = fetch_adzuna_data(category)
                if data:
                    total_records += 1
                    adzuna_results[category] = data
                    continue
            except (
                ImportError,
                AttributeError,
                ValueError,
                KeyError,
                TypeError,
                OSError,
            ) as e:
                logger.debug(f"Adzuna failed for '{category}': {e}")

            # Fallback 2: Jooble API
            try:
                from api_enrichment import fetch_jooble_data

                data = fetch_jooble_data(category)
                if data:
                    total_records += 1
                    if isinstance(data, dict):
                        data["_source"] = "jooble_fallback"
                    adzuna_results[category] = data
                    logger.info(f"Jooble fallback succeeded for '{category}'")
                    continue
            except (
                ImportError,
                AttributeError,
                ValueError,
                KeyError,
                TypeError,
                OSError,
            ) as e:
                logger.debug(f"Jooble fallback failed for '{category}': {e}")

            # Fallback 3: RemoteOK (works for engineering/marketing)
            try:
                from api_enrichment import fetch_remoteok_data

                data = fetch_remoteok_data(category)
                if data:
                    total_records += 1
                    if isinstance(data, dict):
                        data["_source"] = "remoteok_fallback"
                    adzuna_results[category] = data
                    logger.info(f"RemoteOK fallback succeeded for '{category}'")
                    continue
            except (
                ImportError,
                AttributeError,
                ValueError,
                KeyError,
                TypeError,
                OSError,
            ) as e:
                logger.debug(f"RemoteOK fallback failed for '{category}': {e}")

            logger.warning(f"All job data sources failed for '{category}'")

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
            "Enriched job data: %d/%d categories (with fallbacks)",
            total_records,
            len(categories),
        )

    def _enrich_census_demographics(self) -> None:
        """Refresh demographic data (monthly).

        Fallback chain:
          1. Census Bureau API (primary)
          2. BEA API (already connected, free)
          3. Stale Supabase data (do nothing, keep existing)

        Writes to Supabase knowledge_base table under 'census_demographics' category.
        """
        if not self._is_stale("census_demographics"):
            return

        data: Any = None
        source = "census"

        # Fallback 1: Census Bureau API
        try:
            from api_enrichment import fetch_census_data

            data = fetch_census_data()
            if data:
                source = "census"
        except (
            ImportError,
            AttributeError,
            ValueError,
            KeyError,
            TypeError,
            OSError,
        ) as e:
            logger.debug(f"Census API failed: {e}")

        # Fallback 2: BEA API
        if not data:
            try:
                from api_enrichment import fetch_bea_data

                bea_result = fetch_bea_data()
                if bea_result:
                    data = bea_result
                    source = "bea_fallback"
                    logger.info("Census fallback: using BEA data")
            except (
                ImportError,
                AttributeError,
                ValueError,
                KeyError,
                TypeError,
                OSError,
            ) as e:
                logger.debug(f"BEA fallback failed: {e}")

        # Fallback 3: Keep stale data (just log, don't mark as refreshed)
        if not data:
            logger.warning("All demographics sources failed -- keeping stale data")
            self._mark_refreshed("census_demographics", False)
            return

        # Write to Supabase
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
                            else {"value": value, "_source": source}
                        ),
                    }
                )
        elif isinstance(data, list):
            sb_rows.append(
                {
                    "category": "census_demographics",
                    "key": "demographics",
                    "data": {"records": data, "scraped_at": now_iso, "_source": source},
                }
            )
        if sb_rows:
            _upsert_to_supabase("knowledge_base", sb_rows)

        self._mark_refreshed("census_demographics", True, len(data) if data else 0)
        logger.info(
            "Enriched demographics via %s: %d records", source, len(data) if data else 0
        )

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
        """Refresh job posting volumes for popular roles.

        Fallback chain:
          1. Adzuna API (already connected, fast, free)
          2. BLS JOLTS data (national-level, free)
          3. Firecrawl scraping (only if credits available)

        Writes to Supabase knowledge_base table.
        """
        if not self._is_stale("job_posting_volume"):
            return
        now_iso = datetime.now(timezone.utc).isoformat()
        total = 0
        volume_data: dict[str, Any] = {}

        for role in _TOP_SALARY_ROLES[:5]:
            try:
                data: dict[str, Any] | None = None

                # Fallback 1: Adzuna (fast, free, per-role)
                try:
                    from api_enrichment import fetch_adzuna_data

                    adzuna_result = fetch_adzuna_data(role)
                    if adzuna_result and isinstance(adzuna_result, dict):
                        count = (
                            adzuna_result.get("count")
                            or adzuna_result.get("total")
                            or 0
                        )
                        if count > 0:
                            data = {
                                "role": role,
                                "location": "",
                                "estimated_openings": count,
                                "sources": ["adzuna"],
                                "trend": "stable",
                                "source": "adzuna",
                                "last_updated": now_iso,
                            }
                            logger.info(f"Job volume via Adzuna for '{role}': {count}")
                except (ImportError, AttributeError, ValueError, OSError) as e:
                    logger.debug(f"Adzuna fallback failed for '{role}': {e}")

                # Fallback 2: Firecrawl/router scraping (only if Adzuna missed)
                if not data:
                    try:
                        from firecrawl_enrichment import scrape_job_posting_volume

                        scrape_result = scrape_job_posting_volume(role)
                        if (
                            scrape_result
                            and scrape_result.get("estimated_openings", 0) > 0
                        ):
                            data = scrape_result
                            logger.info(
                                f"Job volume via scraper for '{role}': {data.get('estimated_openings')}"
                            )
                    except (ImportError, AttributeError, ValueError, OSError) as e:
                        logger.debug(f"Scraper fallback failed for '{role}': {e}")

                if data and data.get("estimated_openings", 0) > 0:
                    total += 1
                    volume_data[role] = data

            except (ValueError, KeyError, TypeError, OSError) as e:
                logger.error(
                    "Job posting volume failed for '%s': %s",
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
        logger.info("Enriched job posting volumes: %d roles (with fallbacks)", total)

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

    def _enrich_benchmark_drift_check(self) -> None:
        """Monthly task: compare live CPC/CPA data against stored benchmarks.

        Loads the stored Google Ads and external benchmark files, fetches
        current Adzuna CPC/CPA data via api_enrichment, and flags any
        metric that has drifted more than 20% from the stored value.

        Also runs quarterly file staleness checks on all benchmark JSON
        files and alerts when Adzuna/BLS data differs significantly from
        stored benchmarks (>25% drift flagged as critical).

        Results are written to a local JSON file and optionally to Supabase.
        """
        if not self._is_stale("benchmark_drift_check"):
            return

        # Quarterly benchmark file freshness check (Issue 6)
        try:
            freshness = check_benchmark_freshness()
            stale_files = [
                f"{fname} ({info.get('age_days', '?')}d old)"
                for fname, info in freshness.items()
                if info.get("stale")
            ]
            if stale_files:
                logger.warning(
                    "Quarterly benchmark freshness check: %d stale file(s): %s",
                    len(stale_files),
                    ", ".join(stale_files),
                )
                send_alert(
                    subject=f"Benchmark Staleness: {len(stale_files)} file(s) older than 90 days",
                    body=(
                        f"<p>The following benchmark files need refreshing:</p>"
                        f"<ul>{''.join(f'<li>{f}</li>' for f in stale_files)}</ul>"
                        f"<p>Stale benchmarks may lead to inaccurate CPC/CPA "
                        f"recommendations in generated media plans.</p>"
                    ),
                    severity="warning",
                )
            else:
                logger.info("Quarterly benchmark freshness check: all files current")
        except (ValueError, OSError) as exc:
            logger.error("Benchmark freshness check failed: %s", exc, exc_info=True)

        drift_threshold = 0.20  # 20% drift triggers a flag
        drift_results: list[dict[str, Any]] = []

        try:
            # Load stored benchmarks
            stored_benchmarks: dict[str, dict[str, Any]] = {}
            for fname in _BENCHMARK_FILES:
                fpath = DATA_DIR / fname
                try:
                    if fpath.exists():
                        raw = json.loads(fpath.read_text(encoding="utf-8"))
                        stored_benchmarks[fname] = raw
                except (json.JSONDecodeError, OSError) as exc:
                    logger.warning(
                        "Could not load benchmark file %s for drift check: %s",
                        fname,
                        exc,
                    )

            # Extract stored CPC/CPA values per industry from Google Ads benchmarks
            google_bench = stored_benchmarks.get("google_ads_2025_benchmarks.json", {})
            industries_to_check: list[str] = []
            stored_cpc_map: dict[str, float] = {}
            stored_cpa_map: dict[str, float] = {}

            if isinstance(google_bench, dict):
                for industry, metrics in google_bench.items():
                    if isinstance(metrics, dict) and metrics.get("avg_cpc"):
                        industries_to_check.append(industry)
                        stored_cpc_map[industry] = float(metrics.get("avg_cpc") or 0)
                        stored_cpa_map[industry] = float(metrics.get("avg_cpa") or 0)

            # Fetch live Adzuna data for comparison
            live_cpc_map: dict[str, float] = {}
            try:
                from api_enrichment import fetch_job_market

                for industry in industries_to_check[
                    :5
                ]:  # limit to 5 to avoid rate limits
                    try:
                        live_data = fetch_job_market(
                            roles=[industry], locations=["United States"]
                        )
                        if live_data and isinstance(live_data, dict):
                            for _role, role_data in live_data.items():
                                if isinstance(role_data, dict):
                                    live_cpc = role_data.get("avg_salary") or 0
                                    if live_cpc:
                                        live_cpc_map[industry] = float(live_cpc)
                    except (ValueError, KeyError, TypeError, OSError) as exc:
                        logger.debug("Adzuna fetch for %s failed: %s", industry, exc)
            except ImportError:
                logger.warning("api_enrichment not available for drift check")

            # Compare and flag drifts
            for industry in industries_to_check:
                stored_cpc = stored_cpc_map.get(industry, 0)
                live_cpc = live_cpc_map.get(industry)

                if stored_cpc > 0 and live_cpc and live_cpc > 0:
                    pct_change = (live_cpc - stored_cpc) / stored_cpc
                    drifted = abs(pct_change) > drift_threshold
                    entry = {
                        "industry": industry,
                        "metric": "cpc",
                        "stored_value": round(stored_cpc, 2),
                        "live_value": round(live_cpc, 2),
                        "pct_change": round(pct_change * 100, 1),
                        "drifted": drifted,
                        "checked_at": datetime.now(timezone.utc).isoformat(),
                    }
                    drift_results.append(entry)

                    if drifted:
                        logger.warning(
                            "Benchmark drift detected: %s CPC for %s: "
                            "stored=$%.2f, live=$%.2f (%+.1f%%)",
                            "Google Ads",
                            industry,
                            stored_cpc,
                            live_cpc,
                            pct_change * 100,
                        )

            # Write drift results to local JSON
            drift_path = DATA_DIR / "benchmark_drift_results.json"
            try:
                drift_path.write_text(
                    json.dumps(
                        {
                            "checked_at": datetime.now(timezone.utc).isoformat(),
                            "drift_threshold_pct": drift_threshold * 100,
                            "results": drift_results,
                            "total_checked": len(drift_results),
                            "total_drifted": sum(
                                1 for r in drift_results if r.get("drifted")
                            ),
                        },
                        indent=2,
                        default=str,
                    ),
                    encoding="utf-8",
                )
            except OSError as exc:
                logger.error("Failed to write drift results: %s", exc, exc_info=True)

            # Upsert to Supabase knowledge_base
            if drift_results:
                sb_rows = [
                    {
                        "category": "benchmark_drift",
                        "key": f"{r['industry']}_{r['metric']}",
                        "data": r,
                    }
                    for r in drift_results
                    if r.get("drifted")
                ]
                if sb_rows:
                    _upsert_to_supabase("knowledge_base", sb_rows)

            drifted_count = sum(1 for r in drift_results if r.get("drifted"))
            self._mark_refreshed("benchmark_drift_check", True, len(drift_results))
            logger.info(
                "Benchmark drift check complete: %d industries checked, "
                "%d drifted (>%.0f%%)",
                len(drift_results),
                drifted_count,
                drift_threshold * 100,
            )

            # Alert if significant drift detected
            if drifted_count > 0:
                drifted_items = [r for r in drift_results if r.get("drifted")]
                drift_summary = "; ".join(
                    f"{r['industry']}: {r['pct_change']:+.1f}%"
                    for r in drifted_items[:5]
                )
                # Flag critical drifts (>25%) from Adzuna/BLS data
                critical_drifts = [
                    r for r in drifted_items if abs(r.get("pct_change") or 0) > 25
                ]
                severity = "critical" if critical_drifts else "warning"
                if critical_drifts:
                    logger.error(
                        "Critical benchmark drift: %d metrics drifted >25%% from "
                        "stored benchmarks. Adzuna/BLS data significantly differs. "
                        "Consider updating benchmark files.",
                        len(critical_drifts),
                    )
                send_alert(
                    subject=f"Benchmark Drift: {drifted_count} industry benchmarks drifted >20%",
                    body=(
                        f"<p><b>{drifted_count}</b> benchmarks have drifted beyond the "
                        f"{drift_threshold * 100:.0f}% threshold.</p>"
                        f"<p>Top drifts: {drift_summary}</p>"
                        + (
                            f"<p><strong>CRITICAL:</strong> {len(critical_drifts)} metric(s) "
                            f"drifted >25%% -- Adzuna/BLS data significantly differs from "
                            f"stored benchmarks. Update benchmark JSON files.</p>"
                            if critical_drifts
                            else ""
                        )
                        + f"<p>Full results: <code>data/benchmark_drift_results.json</code></p>"
                    ),
                    severity=severity,
                )

        except (ValueError, KeyError, TypeError, OSError, RuntimeError) as exc:
            logger.error("Benchmark drift check failed: %s", exc, exc_info=True)
            self._mark_refreshed("benchmark_drift_check", False)

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
            # Monthly benchmark drift check (CPC/CPA vs stored benchmarks)
            ("benchmark_drift_check", self._enrich_benchmark_drift_check),
        ]

        results: dict = {
            "checked": 0,
            "refreshed": 0,
            "skipped": 0,
            "failed": 0,
        }

        failed_source_names: list[str] = []

        for source, task_fn in tasks:
            results["checked"] += 1
            if not self._is_stale(source):
                results["skipped"] += 1
                continue
            try:
                task_fn()
                # Check the latest log entry (now populated by _mark_refreshed)
                if (
                    self._enrichment_log
                    and self._enrichment_log[-1].get("source") == source
                ):
                    if self._enrichment_log[-1].get("success"):
                        results["refreshed"] += 1
                    else:
                        results["failed"] += 1
                        failed_source_names.append(source)
                else:
                    # Task didn't call _mark_refreshed -- assume success
                    results["refreshed"] += 1
            except (ValueError, KeyError, TypeError, OSError, RuntimeError) as e:
                results["failed"] += 1
                failed_source_names.append(source)
                self._mark_refreshed(source, success=False)
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

        # Alert on enrichment failures
        failed_count = results.get("failed") or 0
        if failed_count > 0:
            # Use the tracked failed source names from this cycle
            recent_failures = (
                failed_source_names if failed_source_names else ["unknown"]
            )
            send_alert(
                subject=f"Data Enrichment: {failed_count} source(s) failed",
                body=(
                    f"<p><b>{failed_count}</b> enrichment source(s) failed after retries.</p>"
                    f"<p>Failed sources: {', '.join(recent_failures)}</p>"
                    f"<p>Checked: {results.get('checked') or 0} | "
                    f"Refreshed: {results.get('refreshed') or 0} | "
                    f"Skipped: {results.get('skipped') or 0}</p>"
                    f"<p>Elapsed: {elapsed}s</p>"
                    f"<p>Check: <code>/api/health/enrichment</code></p>"
                ),
                severity="critical" if failed_count >= 3 else "warning",
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

            # Benchmark file freshness (runs outside the lock-critical path)
            benchmark_fresh = check_benchmark_freshness()
            any_stale_benchmarks = any(
                v.get("stale", False) for v in benchmark_fresh.values()
            )

            return {
                "running": self._running,
                "supabase_enabled": _SUPABASE_ENABLED,
                "state": {
                    "last_runs": dict(self._state.get("last_runs", {})),
                    "stats": dict(self._state.get("stats", {})),
                },
                "recent_log": list(self._enrichment_log[-10:]),
                "freshness": freshness,
                "benchmark_freshness": benchmark_fresh,
                "benchmark_stale_warning": any_stale_benchmarks,
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
