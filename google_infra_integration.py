"""Google Cloud Infrastructure Integration -- Logging, Monitoring, Apps Script.

Three integrations for Nova AI Suite, all using GOOGLE_SLIDES_CREDENTIALS_B64:
  1. Cloud Logging  (50 GB/mo free) -- entries:write, entries:list
  2. Cloud Monitoring (150 MB free) -- projects/{project}/timeSeries
  3. Apps Script -- scripts/{scriptId}:run for Nova branding

Env vars: GOOGLE_SLIDES_CREDENTIALS_B64, GCP_PROJECT_ID, APPS_SCRIPT_ID
Dependencies: stdlib only.  Thread-safe.
"""

from __future__ import annotations

import base64, json, logging, os, subprocess, tempfile, threading, time
import urllib.error, urllib.parse, urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_GCP_PROJECT = os.environ.get("GCP_PROJECT_ID") or "gen-lang-client-0603536849"
_APPS_SCRIPT_ID = os.environ.get("APPS_SCRIPT_ID") or ""
_LOGGING_WRITE = "https://logging.googleapis.com/v2/entries:write"
_LOGGING_LIST = "https://logging.googleapis.com/v2/entries:list"
_MONITORING_TS = (
    f"https://monitoring.googleapis.com/v3/projects/{_GCP_PROJECT}/timeSeries"
)
_SCRIPT_RUN = "https://script.googleapis.com/v1/scripts/{sid}:run"
_TOKEN_URI = "https://oauth2.googleapis.com/token"
_LOG_NAME = f"projects/{_GCP_PROJECT}/logs/nova-ai-suite"
_VALID_SEVERITIES = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "DEFAULT"}
_SCOPES = [
    "https://www.googleapis.com/auth/logging.write",
    "https://www.googleapis.com/auth/logging.read",
    "https://www.googleapis.com/auth/monitoring.write",
    "https://www.googleapis.com/auth/monitoring.read",
    "https://www.googleapis.com/auth/script.projects",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/presentations",
]
_NOVA_BRAND = {
    "PORT_GORE": "#202058",
    "BLUE_VIOLET": "#5A54BD",
    "DOWNY_TEAL": "#6BB3CD",
}

_token_lock = threading.Lock()
_token_cache: Dict[str, Any] = {"token": None, "expires_at": 0.0}

# ── Auth helpers ─────────────────────────────────────────────────────────────


def _load_credentials() -> Optional[Dict[str, str]]:
    """Load service-account JSON from GOOGLE_SLIDES_CREDENTIALS_B64."""
    b64 = os.environ.get("GOOGLE_SLIDES_CREDENTIALS_B64") or ""
    if not b64:
        return None
    try:
        creds = json.loads(base64.b64decode(b64))
        for f in ("client_email", "private_key", "token_uri"):
            if f not in creds:
                logger.error("Service account JSON missing field: %s", f)
                return None
        return creds
    except Exception as exc:
        logger.error("Failed to decode B64 credentials: %s", exc, exc_info=True)
        return None


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _build_jwt(creds: Dict[str, str]) -> str:
    """Build signed RS256 JWT. Tries cryptography lib, then openssl CLI."""
    now = int(time.time())
    hdr = _b64url(
        json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode()
    )
    clm = _b64url(
        json.dumps(
            {
                "iss": creds["client_email"],
                "scope": " ".join(_SCOPES),
                "aud": creds.get("token_uri") or _TOKEN_URI,
                "iat": now,
                "exp": now + 3600,
            },
            separators=(",", ":"),
        ).encode()
    )
    signing_input = f"{hdr}.{clm}".encode("ascii")
    pem = creds["private_key"]

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
        sig = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())  # type: ignore[union-attr]
        return f"{hdr}.{clm}.{_b64url(sig)}"
    except ImportError:
        pass

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as kf:
            kf.write(pem)
            kp = kf.name
        try:
            proc = subprocess.run(
                ["openssl", "dgst", "-sha256", "-sign", kp],
                input=signing_input,
                capture_output=True,
                timeout=10,
            )
            if proc.returncode == 0 and proc.stdout:
                return f"{hdr}.{clm}.{_b64url(proc.stdout)}"
        finally:
            try:
                os.unlink(kp)
            except OSError:
                pass
    except FileNotFoundError:
        pass
    raise RuntimeError(
        "Cannot sign JWT: install 'cryptography' or ensure 'openssl' on PATH"
    )


def _get_access_token() -> Optional[str]:
    """Obtain a cached OAuth2 access token. Thread-safe, refreshes 5 min before expiry."""
    now = time.time()
    with _token_lock:
        if _token_cache["token"] and _token_cache["expires_at"] > now + 300:
            return _token_cache["token"]
    creds = _load_credentials()
    if not creds:
        return None
    try:
        body = urllib.parse.urlencode(
            {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": _build_jwt(creds),
            }
        ).encode()
        req = urllib.request.Request(
            creds.get("token_uri") or _TOKEN_URI,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        with _token_lock:
            _token_cache["token"] = data.get("access_token") or ""
            _token_cache["expires_at"] = time.time() + int(data.get("expires_in", 3600))
        return _token_cache["token"]
    except Exception as exc:
        logger.error("OAuth2 token exchange failed: %s", exc, exc_info=True)
        return None


def _api_request(
    url: str, payload: dict | None = None, method: str = "POST", timeout: int = 15
) -> Optional[dict]:
    """Authenticated request to a Google API endpoint. Returns parsed JSON or None."""
    token = _get_access_token()
    if not token:
        logger.warning("No access token -- skipping %s %s", method, url)
        return None
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        err = ""
        try:
            err = exc.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            pass
        logger.error(
            "Google API %s %s -> %s: %s", method, url, exc.code, err, exc_info=True
        )
    except Exception as exc:
        logger.error("Google API request failed: %s", exc, exc_info=True)
    return None


# ── Cloud Logging ────────────────────────────────────────────────────────────


def write_log(severity: str, message: str, labels: dict | None = None) -> bool:
    """Write a structured log entry to Cloud Logging.

    Args:
        severity: DEBUG/INFO/WARNING/ERROR/CRITICAL.
        message: Log message text.
        labels: Optional key-value labels.
    Returns:
        True if written successfully.
    """
    severity = severity.upper()
    if severity not in _VALID_SEVERITIES:
        severity = "DEFAULT"
    resource = {"type": "global", "labels": {"project_id": _GCP_PROJECT}}
    entry: Dict[str, Any] = {
        "logName": _LOG_NAME,
        "resource": resource,
        "severity": severity,
        "textPayload": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if labels:
        entry["labels"] = {str(k): str(v) for k, v in labels.items()}
    result = _api_request(
        _LOGGING_WRITE, {"logName": _LOG_NAME, "resource": resource, "entries": [entry]}
    )
    return result is not None


def query_logs(filter_str: str, hours: int = 24, limit: int = 100) -> list[dict]:
    """Query recent log entries. Returns list of entries, newest first.

    Args:
        filter_str: Cloud Logging filter expression.
        hours: Look-back window (default 24).
        limit: Max entries (default 100, capped 1000).
    """
    limit = min(max(1, limit), 1000)
    cutoff = datetime.fromtimestamp(
        time.time() - hours * 3600, tz=timezone.utc
    ).isoformat()
    filt = f'logName="{_LOG_NAME}" AND timestamp>="{cutoff}"'
    if filter_str:
        filt += f" AND ({filter_str})"
    result = _api_request(
        _LOGGING_LIST,
        {
            "resourceNames": [f"projects/{_GCP_PROJECT}"],
            "filter": filt,
            "orderBy": "timestamp desc",
            "pageSize": limit,
        },
    )
    return result.get("entries", []) if result else []


# ── Cloud Monitoring ─────────────────────────────────────────────────────────


def write_metric(metric_type: str, value: float, labels: dict | None = None) -> bool:
    """Write a custom metric data point to Cloud Monitoring.

    Args:
        metric_type: e.g. 'nova/api_latency_ms' (auto-prefixed with custom.googleapis.com/).
        value: Numeric value.
        labels: Optional metric labels.
    Returns:
        True if written successfully.
    """
    if not metric_type.startswith("custom.googleapis.com/"):
        metric_type = f"custom.googleapis.com/{metric_type}"
    now_iso = datetime.now(timezone.utc).isoformat()
    ml = {str(k): str(v) for k, v in labels.items()} if labels else {}
    ts = {
        "metric": {"type": metric_type, "labels": ml},
        "resource": {"type": "global", "labels": {"project_id": _GCP_PROJECT}},
        "points": [
            {"interval": {"endTime": now_iso}, "value": {"doubleValue": float(value)}}
        ],
    }
    result = _api_request(_MONITORING_TS, {"timeSeries": [ts]})
    return result is not None


def get_metric(metric_type: str, hours: int = 1) -> list[dict]:
    """Read metric time series from Cloud Monitoring.

    Args:
        metric_type: Metric name (auto-prefixed).
        hours: Look-back window (default 1).
    Returns:
        List of time series dicts.
    """
    if not metric_type.startswith("custom.googleapis.com/"):
        metric_type = f"custom.googleapis.com/{metric_type}"
    now = datetime.now(timezone.utc)
    start = datetime.fromtimestamp(now.timestamp() - hours * 3600, tz=timezone.utc)
    params = urllib.parse.urlencode(
        {
            "filter": f'metric.type="{metric_type}"',
            "interval.startTime": start.isoformat(),
            "interval.endTime": now.isoformat(),
        }
    )
    result = _api_request(f"{_MONITORING_TS}?{params}", method="GET")
    return result.get("timeSeries", []) if result else []


# ── Apps Script ──────────────────────────────────────────────────────────────


def _run_script(function_name: str, parameters: list[Any]) -> Optional[dict]:
    """Execute an Apps Script function via the Execution API."""
    if not _APPS_SCRIPT_ID:
        logger.warning("APPS_SCRIPT_ID not set -- cannot run Apps Script")
        return None
    url = _SCRIPT_RUN.format(sid=_APPS_SCRIPT_ID)
    return _api_request(
        url, {"function": function_name, "parameters": parameters}, timeout=30
    )


def format_spreadsheet(spreadsheet_id: str, template_name: str = "nova_brand") -> bool:
    """Apply Nova branding to a Google Sheet via Apps Script.

    Applies header colours, fonts, conditional formatting, and column auto-resize.
    """
    result = _run_script(
        "formatSpreadsheet", [spreadsheet_id, template_name, _NOVA_BRAND]
    )
    if result is None:
        return False
    if "error" in result:
        logger.error(
            "formatSpreadsheet error: %s",
            result["error"].get("message", result["error"]),
        )
        return False
    logger.info("Spreadsheet %s formatted (%s)", spreadsheet_id, template_name)
    return True


def format_presentation(
    presentation_id: str, template_name: str = "nova_brand"
) -> bool:
    """Apply Nova branding to a Google Slides presentation via Apps Script.

    Applies master slide colours, Inter font, and Nova logo placement.
    """
    result = _run_script(
        "formatPresentation", [presentation_id, template_name, _NOVA_BRAND]
    )
    if result is None:
        return False
    if "error" in result:
        logger.error(
            "formatPresentation error: %s",
            result["error"].get("message", result["error"]),
        )
        return False
    logger.info("Presentation %s formatted (%s)", presentation_id, template_name)
    return True


# ── Health check ─────────────────────────────────────────────────────────────


def get_status() -> dict:
    """Health check for all three integrations. Returns dict with per-service booleans."""
    status: Dict[str, Any] = {
        "cloud_logging": False,
        "cloud_monitoring": False,
        "apps_script": False,
        "project_id": _GCP_PROJECT,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    has_creds = _load_credentials() is not None
    has_token = _get_access_token() is not None
    status["credentials_configured"] = has_creds
    status["token_available"] = has_token

    if has_token:
        try:
            r = _api_request(
                _LOGGING_LIST,
                {
                    "resourceNames": [f"projects/{_GCP_PROJECT}"],
                    "filter": f'logName="{_LOG_NAME}"',
                    "pageSize": 1,
                },
            )
            status["cloud_logging"] = r is not None
        except Exception:
            pass
        try:
            now = datetime.now(timezone.utc).isoformat()
            p = urllib.parse.urlencode(
                {
                    "filter": 'metric.type="custom.googleapis.com/nova/health"',
                    "interval.startTime": now,
                    "interval.endTime": now,
                }
            )
            status["cloud_monitoring"] = (
                _api_request(f"{_MONITORING_TS}?{p}", method="GET") is not None
            )
        except Exception:
            pass
        status["apps_script"] = bool(_APPS_SCRIPT_ID)

    status["healthy"] = (
        status["cloud_logging"] and status["cloud_monitoring"] and has_creds
    )
    return status
