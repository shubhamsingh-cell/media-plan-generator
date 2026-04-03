"""Google BigQuery integration for Nova AI Suite historical analytics.

Uses BigQuery REST API v2 with service account from GOOGLE_SLIDES_CREDENTIALS_B64.
Stdlib only (urllib.request, json, ssl). Reuses _get_access_token from sheets_export.

Env vars: GOOGLE_BIGQUERY_PROJECT (or service account project_id fallback),
          GOOGLE_SLIDES_CREDENTIALS_B64 (base64-encoded service account JSON).
"""

from __future__ import annotations
import json, logging, os, ssl, threading, time, urllib.error, urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)
_BQ = "https://bigquery.googleapis.com/bigquery/v2"
_DS = "nova_analytics"
_T_PLANS = "media_plans"
_T_BM = "benchmarks"
_stats: dict[str, Any] = {
    "queries": 0,
    "bytes": 0,
    "last_at": None,
    "lock": threading.Lock(),
}


def _get_access_token() -> Optional[str]:
    """Obtain Google OAuth2 access token via sheets_export."""
    try:
        from sheets_export import _get_access_token as _t

        return _t()
    except ImportError:
        logger.error("sheets_export not found -- cannot authenticate BigQuery")
        return None


def _get_project_id() -> Optional[str]:
    """Resolve GCP project ID from env or service account JSON."""
    proj = os.environ.get("GOOGLE_BIGQUERY_PROJECT") or ""
    if proj:
        return proj
    import base64

    for src in [os.environ.get("GOOGLE_SLIDES_CREDENTIALS_B64") or ""]:
        if src:
            try:
                return json.loads(base64.b64decode(src)).get("project_id") or None
            except Exception as exc:
                logger.error(
                    "Failed reading project_id from B64 creds: %s", exc, exc_info=True
                )
    cred_path = os.environ.get("GOOGLE_SHEETS_CREDENTIALS") or ""
    if cred_path and os.path.isfile(cred_path):
        try:
            with open(cred_path, "r", encoding="utf-8") as fh:
                return json.load(fh).get("project_id") or None
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Failed reading project_id from file: %s", exc, exc_info=True)
    return None


def _bq_req(
    method: str,
    url: str,
    body: Optional[dict] = None,
    token: Optional[str] = None,
    timeout: int = 30,
) -> Optional[dict]:
    """Make authenticated BigQuery REST API request."""
    if token is None:
        token = _get_access_token()
    if not token:
        return None
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(
            req, context=ssl.create_default_context(), timeout=timeout
        ) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err = ""
        try:
            err = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error(
            "BigQuery %s %s -> %d: %s", method, url, exc.code, err, exc_info=True
        )
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        logger.error("BigQuery request failed: %s", exc, exc_info=True)
    return None


def _track(bytes_processed: int) -> None:
    with _stats["lock"]:
        _stats["queries"] += 1
        _stats["bytes"] += bytes_processed
        _stats["last_at"] = datetime.now(timezone.utc).isoformat()


def _run_query(sql: str, params: dict[str, dict] | None = None) -> list[dict]:
    """Execute parameterized BigQuery SQL, return rows as dicts."""
    project = _get_project_id()
    if not project:
        logger.error("BigQuery project ID not configured")
        return []
    qp = [{"name": n, **s} for n, s in (params or {}).items()]
    body: dict[str, Any] = {
        "query": sql,
        "useLegacySql": False,
        "defaultDataset": {"projectId": project, "datasetId": _DS},
        "maxResults": 1000,
        "maximumBytesBilled": str(1_073_741_824),
    }
    if qp:
        body["parameterMode"] = "NAMED"
        body["queryParameters"] = qp
    result = _bq_req("POST", f"{_BQ}/projects/{project}/queries", body=body, timeout=60)
    if not result:
        return []
    bp = int(result.get("totalBytesProcessed") or 0)
    _track(bp)
    fields = result.get("schema", {}).get("fields", [])
    rows: list[dict] = []
    for raw in result.get("rows", []):
        row: dict[str, Any] = {}
        for i, f in enumerate(fields):
            cell = raw.get("f", [])[i] if i < len(raw.get("f", [])) else {}
            v = cell.get("v")
            if v is not None:
                ft = f.get("type", "").upper()
                if ft in ("FLOAT", "FLOAT64"):
                    try:
                        v = float(v)
                    except (ValueError, TypeError):
                        pass
                elif ft in ("INTEGER", "INT64"):
                    try:
                        v = int(v)
                    except (ValueError, TypeError):
                        pass
            row[f["name"]] = v
        rows.append(row)
    logger.info("BigQuery: %d rows, %s bytes", len(rows), f"{bp:,}")
    return rows


def _mk_table(project: str, tid: str, schema: dict, token: str) -> bool:
    """Create table if not exists (idempotent)."""
    body = {
        "tableReference": {"projectId": project, "datasetId": _DS, "tableId": tid},
        "schema": schema,
        "timePartitioning": {"type": "DAY", "field": "created_at"},
    }
    r = _bq_req(
        "POST",
        f"{_BQ}/projects/{project}/datasets/{_DS}/tables",
        body=body,
        token=token,
    )
    if r is None:  # may be 409 already exists
        return (
            _bq_req(
                "GET",
                f"{_BQ}/projects/{project}/datasets/{_DS}/tables/{tid}",
                token=token,
            )
            is not None
        )
    return True


# ---- Schemas (inline) ----
_PLAN_FIELDS = [
    {"name": "plan_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "client_name", "type": "STRING"},
    {"name": "industry", "type": "STRING"},
    {"name": "job_title", "type": "STRING"},
    {"name": "budget", "type": "FLOAT"},
    {"name": "locations", "type": "STRING"},
    {"name": "channels_json", "type": "STRING"},
    {"name": "channel_count", "type": "INTEGER"},
    {"name": "quality_score", "type": "FLOAT"},
    {"name": "llm_model", "type": "STRING"},
    {"name": "generation_time_ms", "type": "INTEGER"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]
_BM_FIELDS = [
    {"name": "benchmark_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "industry", "type": "STRING"},
    {"name": "job_category", "type": "STRING"},
    {"name": "channel", "type": "STRING"},
    {"name": "cpc", "type": "FLOAT"},
    {"name": "cpa", "type": "FLOAT"},
    {"name": "apply_rate", "type": "FLOAT"},
    {"name": "source", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


# ---- Public API ----


def store_media_plan(plan_data: dict) -> bool:
    """Insert a media plan record into BigQuery (streaming insert)."""
    project = _get_project_id()
    if not project:
        logger.error("BigQuery project ID not configured")
        return False
    channels = (
        plan_data.get("channels") or plan_data.get("channel_recommendations") or []
    )
    budget_raw = str(plan_data.get("budget") or plan_data.get("monthly_budget") or 0)
    try:
        budget_val = float(budget_raw.replace("$", "").replace(",", "") or 0)
    except (ValueError, TypeError):
        budget_val = 0.0
    pid = plan_data.get("plan_id") or f"plan_{int(time.time())}"
    row = {
        "plan_id": pid,
        "client_name": plan_data.get("client_name") or "",
        "industry": plan_data.get("industry") or "",
        "job_title": plan_data.get("job_title") or plan_data.get("role") or "",
        "budget": budget_val,
        "locations": json.dumps(plan_data.get("locations") or []),
        "channels_json": json.dumps(channels),
        "channel_count": len(channels),
        "quality_score": plan_data.get("quality_score")
        or plan_data.get("score")
        or None,
        "llm_model": plan_data.get("llm_model") or plan_data.get("model") or "",
        "generation_time_ms": plan_data.get("generation_time_ms") or None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    url = f"{_BQ}/projects/{project}/datasets/{_DS}/tables/{_T_PLANS}/insertAll"
    r = _bq_req("POST", url, body={"rows": [{"insertId": pid, "json": row}]})
    if r is None:
        return False
    if r.get("insertErrors"):
        logger.error("BQ insert errors: %s", json.dumps(r["insertErrors"]))
        return False
    logger.info("Stored media plan %s in BigQuery", pid)
    return True


def store_benchmark(benchmark_data: dict) -> bool:
    """Insert a benchmark data point into BigQuery (streaming insert)."""
    project = _get_project_id()
    if not project:
        logger.error("BigQuery project ID not configured")
        return False
    bid = benchmark_data.get("benchmark_id") or f"bm_{int(time.time())}"
    row = {
        "benchmark_id": bid,
        "industry": benchmark_data.get("industry") or "",
        "job_category": benchmark_data.get("job_category") or "",
        "channel": benchmark_data.get("channel") or "",
        "cpc": benchmark_data.get("cpc"),
        "cpa": benchmark_data.get("cpa"),
        "apply_rate": benchmark_data.get("apply_rate"),
        "source": benchmark_data.get("source") or "nova",
        "period": benchmark_data.get("period")
        or datetime.now(timezone.utc).strftime("%Y-%m"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    url = f"{_BQ}/projects/{project}/datasets/{_DS}/tables/{_T_BM}/insertAll"
    r = _bq_req("POST", url, body={"rows": [{"insertId": bid, "json": row}]})
    if r is None:
        return False
    if r.get("insertErrors"):
        logger.error("BQ benchmark insert errors: %s", json.dumps(r["insertErrors"]))
        return False
    logger.info("Stored benchmark %s in BigQuery", bid)
    return True


def query_historical_cpc(
    industry: str, job_category: str, months: int = 6
) -> list[dict]:
    """Query historical CPC trends by industry and job category."""
    sql = f"""SELECT period, channel, ROUND(AVG(cpc),2) AS avg_cpc,
        ROUND(AVG(cpa),2) AS avg_cpa, COUNT(*) AS sample_size
        FROM `{_DS}.{_T_BM}`
        WHERE LOWER(industry)=LOWER(@industry) AND LOWER(job_category)=LOWER(@job_category)
          AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(months)} MONTH)
        GROUP BY period, channel ORDER BY period DESC, channel"""
    return _run_query(
        sql,
        {
            "industry": {
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": industry},
            },
            "job_category": {
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": job_category},
            },
        },
    )


def query_plan_performance(client_name: str = "", industry: str = "") -> list[dict]:
    """Query historical plan generation stats, optionally filtered."""
    conds, params = ["1=1"], {}
    if client_name:
        conds.append("LOWER(client_name)=LOWER(@client_name)")
        params["client_name"] = {
            "parameterType": {"type": "STRING"},
            "parameterValue": {"value": client_name},
        }
    if industry:
        conds.append("LOWER(industry)=LOWER(@industry)")
        params["industry"] = {
            "parameterType": {"type": "STRING"},
            "parameterValue": {"value": industry},
        }
    sql = f"""SELECT plan_id, client_name, industry, job_title, budget, channel_count,
        quality_score, llm_model, generation_time_ms, created_at
        FROM `{_DS}.{_T_PLANS}` WHERE {' AND '.join(conds)}
        ORDER BY created_at DESC LIMIT 100"""
    return _run_query(sql, params)


def run_query(sql: str) -> list[dict]:
    """Run arbitrary read-only SQL against nova_analytics. 1 GB billing cap."""
    forbidden = (
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "ALTER",
        "CREATE",
        "MERGE",
        "TRUNCATE",
    )
    up = sql.strip().upper()
    for kw in forbidden:
        if up.startswith(kw):
            raise ValueError(f"Read-only queries only, got: {kw}")
    return _run_query(sql)


def ensure_tables_exist() -> bool:
    """Create nova_analytics dataset + media_plans/benchmarks tables (idempotent)."""
    project = _get_project_id()
    if not project:
        logger.error("BigQuery project ID not configured")
        return False
    token = _get_access_token()
    if not token:
        return False
    _bq_req(
        "POST",
        f"{_BQ}/projects/{project}/datasets",
        token=token,
        body={
            "datasetReference": {"projectId": project, "datasetId": _DS},
            "location": "US",
            "description": "Nova AI Suite analytics data",
        },
    )
    ok1 = _mk_table(project, _T_PLANS, {"fields": _PLAN_FIELDS}, token)
    ok2 = _mk_table(project, _T_BM, {"fields": _BM_FIELDS}, token)
    if ok1 and ok2:
        logger.info("BigQuery tables verified: %s.{%s,%s}", _DS, _T_PLANS, _T_BM)
    return ok1 and ok2


def get_status() -> dict:
    """Health check with query count tracking (1 TB/mo free tier)."""
    project = _get_project_id()
    token = _get_access_token()
    with _stats["lock"]:
        s = {
            "total_queries": _stats["queries"],
            "total_bytes_processed": _stats["bytes"],
            "last_query_at": _stats["last_at"],
        }
    free_tb = 1_099_511_627_776
    return {
        "configured": project is not None and token is not None,
        "project_id": project,
        "dataset": _DS,
        "tables": [_T_PLANS, _T_BM],
        "query_stats": s,
        "free_tier_usage_pct": round(s["total_bytes_processed"] / free_tb * 100, 4),
    }
