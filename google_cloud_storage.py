"""Google Cloud Storage integration for file storage.

Handles generated reports, uploaded files, and KB backups via the
Cloud Storage JSON API v1.  Reuses credential/token helpers from
sheets_export.py (shared GOOGLE_SLIDES_CREDENTIALS_B64 env var).

Env vars: GOOGLE_SLIDES_CREDENTIALS_B64, GOOGLE_STORAGE_BUCKET
Free tier: 5 GB storage | 5K Class A | 50K Class B | 1 GB egress
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sheets_export import _load_credentials, _get_access_token

logger = logging.getLogger(__name__)

_STORAGE_BASE = "https://storage.googleapis.com/storage/v1"
_UPLOAD_BASE = "https://storage.googleapis.com/upload/storage/v1"
_DEFAULT_BUCKET = os.environ.get("GOOGLE_STORAGE_BUCKET") or ""
_FREE_TIER = {
    "storage_bytes": 5 * 1024 * 1024 * 1024,
    "class_a_ops": 5_000,
    "class_b_ops": 50_000,
    "egress_bytes": 1 * 1024 * 1024 * 1024,
}
_usage_lock = threading.Lock()
_usage: Dict[str, int] = {"class_a": 0, "class_b": 0}


def _gcs_request(
    method: str,
    url: str,
    body: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
) -> Optional[bytes]:
    """Authenticated request to the GCS JSON API. Returns bytes or None."""
    token = _get_access_token()
    if not token:
        logger.warning("GCS not configured -- no access token")
        return None
    all_headers = {"Authorization": f"Bearer {token}"}
    if headers:
        all_headers.update(headers)
    req = urllib.request.Request(url, data=body, headers=all_headers, method=method)
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        err = ""
        try:
            err = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("GCS %s %s -> %d: %s", method, url, exc.code, err, exc_info=True)
        return None
    except urllib.error.URLError as exc:
        logger.error("GCS request failed: %s", exc, exc_info=True)
        return None


def _track(op_class: str) -> None:
    """Increment best-effort usage counter."""
    with _usage_lock:
        _usage[op_class] = _usage.get(op_class, 0) + 1


def _bucket(bucket: str) -> str:
    return bucket or _DEFAULT_BUCKET


def upload_file(
    bucket: str = "",
    blob_name: str = "",
    data: bytes = b"",
    content_type: str = "application/octet-stream",
) -> str:
    """Upload file to GCS. Returns media link URL or empty string."""
    bucket = _bucket(bucket)
    if not bucket or not blob_name:
        logger.error("upload_file requires bucket and blob_name")
        return ""
    encoded = urllib.parse.quote(blob_name, safe="")
    url = f"{_UPLOAD_BASE}/b/{bucket}/o?uploadType=media&name={encoded}"
    _track("class_a")
    resp = _gcs_request(
        "POST", url, body=data, headers={"Content-Type": content_type}, timeout=60
    )
    if resp is None:
        return ""
    try:
        obj = json.loads(resp.decode("utf-8"))
        logger.info("Uploaded gs://%s/%s (%d bytes)", bucket, blob_name, len(data))
        return obj.get("mediaLink") or ""
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error("Upload response parse error: %s", exc, exc_info=True)
        return ""


def download_file(bucket: str = "", blob_name: str = "") -> Optional[bytes]:
    """Download file from GCS. Returns bytes or None."""
    bucket = _bucket(bucket)
    if not bucket or not blob_name:
        logger.error("download_file requires bucket and blob_name")
        return None
    encoded = urllib.parse.quote(blob_name, safe="")
    url = f"{_STORAGE_BASE}/b/{bucket}/o/{encoded}?alt=media"
    _track("class_b")
    return _gcs_request("GET", url, timeout=60)


def list_files(bucket: str = "", prefix: str = "") -> List[Dict[str, Any]]:
    """List objects in a GCS bucket. Returns [{name, size, content_type, updated}]."""
    bucket = _bucket(bucket)
    if not bucket:
        logger.error("list_files requires a bucket")
        return []
    params: Dict[str, str] = {"maxResults": "1000"}
    if prefix:
        params["prefix"] = prefix
    url = f"{_STORAGE_BASE}/b/{bucket}/o?{urllib.parse.urlencode(params)}"
    _track("class_b")
    resp = _gcs_request("GET", url)
    if resp is None:
        return []
    try:
        data = json.loads(resp.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error("List response parse error: %s", exc, exc_info=True)
        return []
    return [
        {
            "name": item.get("name") or "",
            "size": int(item.get("size") or 0),
            "content_type": item.get("contentType") or "",
            "updated": item.get("updated") or "",
        }
        for item in (data.get("items") or [])
    ]


def delete_file(bucket: str = "", blob_name: str = "") -> bool:
    """Delete file from GCS. Returns True on success or if already absent."""
    bucket = _bucket(bucket)
    if not bucket or not blob_name:
        logger.error("delete_file requires bucket and blob_name")
        return False
    token = _get_access_token()
    if not token:
        return False
    encoded = urllib.parse.quote(blob_name, safe="")
    url = f"{_STORAGE_BASE}/b/{bucket}/o/{encoded}"
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}"}, method="DELETE"
    )
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=30):
            logger.info("Deleted gs://%s/%s", bucket, blob_name)
            return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            logger.info("gs://%s/%s already absent", bucket, blob_name)
            return True
        logger.error("GCS DELETE %s -> %d", url, exc.code, exc_info=True)
        return False
    except urllib.error.URLError as exc:
        logger.error("GCS DELETE failed: %s", exc, exc_info=True)
        return False


def get_signed_url(
    bucket: str = "",
    blob_name: str = "",
    expiry_minutes: int = 60,
) -> str:
    """Generate a V4 signed URL for temporary access (max 7 days).
    Requires ``cryptography`` package or ``openssl`` CLI."""
    bucket = _bucket(bucket)
    if not bucket or not blob_name:
        logger.error("get_signed_url requires bucket and blob_name")
        return ""
    creds = _load_credentials()
    if not creds:
        return ""
    expiry_seconds = min(expiry_minutes, 10080) * 60
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    client_email = creds.get("client_email") or ""
    credential_scope = f"{datestamp}/auto/storage/goog4_request"
    host = "storage.googleapis.com"
    canonical_uri = f"/{bucket}/{urllib.parse.quote(blob_name, safe='')}"
    qp = {
        "X-Goog-Algorithm": "GOOG4-RSA-SHA256",
        "X-Goog-Credential": f"{client_email}/{credential_scope}",
        "X-Goog-Date": timestamp,
        "X-Goog-Expires": str(expiry_seconds),
        "X-Goog-SignedHeaders": "host",
    }
    canonical_qs = urllib.parse.urlencode(sorted(qp.items()))
    canonical_req = "\n".join(
        [
            "GET",
            canonical_uri,
            canonical_qs,
            f"host:{host}",
            "",
            "host",
            "UNSIGNED-PAYLOAD",
        ]
    )
    string_to_sign = "\n".join(
        [
            "GOOG4-RSA-SHA256",
            timestamp,
            credential_scope,
            hashlib.sha256(canonical_req.encode("utf-8")).hexdigest(),
        ]
    )
    hex_sig = _rsa_sign_hex(
        creds.get("private_key") or "", string_to_sign.encode("utf-8")
    )
    if not hex_sig:
        return ""
    return f"https://{host}{canonical_uri}?{canonical_qs}&X-Goog-Signature={hex_sig}"


def _rsa_sign_hex(pem: str, data: bytes) -> str:
    """RSA-SHA256 sign ``data`` with PEM key. Returns hex or empty string."""
    if not pem:
        logger.error("No private key for signing")
        return ""
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
        return key.sign(data, padding.PKCS1v15(), hashes.SHA256()).hex()  # type: ignore[union-attr]
    except ImportError:
        pass
    import subprocess, tempfile  # noqa: E401

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as kf:
            kf.write(pem)
            kp = kf.name
        try:
            proc = subprocess.run(
                ["openssl", "dgst", "-sha256", "-sign", kp],
                input=data,
                capture_output=True,
                timeout=10,
            )
            if proc.returncode == 0 and proc.stdout:
                return proc.stdout.hex()
        finally:
            try:
                os.unlink(kp)
            except OSError:
                pass
    except FileNotFoundError:
        pass
    logger.error("Cannot sign: no cryptography lib or openssl CLI")
    return ""


def get_status() -> Dict[str, Any]:
    """Health check for GCS integration."""
    creds = _load_credentials()
    with _usage_lock:
        cu = dict(_usage)
    warnings: List[str] = []
    for label, key, lk in [
        ("Class A", "class_a", "class_a_ops"),
        ("Class B", "class_b", "class_b_ops"),
    ]:
        count = cu.get(key, 0)
        limit = _FREE_TIER[lk]
        if count > limit * 0.8:
            warnings.append(
                f"{label} ops: {count}/{limit} ({count / limit * 100:.0f}%)"
            )
    return {
        "configured": creds is not None,
        "service_account": (creds.get("client_email") or "unknown") if creds else None,
        "bucket": _DEFAULT_BUCKET or None,
        "usage": cu,
        "free_tier_limits": _FREE_TIER,
        "free_tier_warnings": warnings,
    }
