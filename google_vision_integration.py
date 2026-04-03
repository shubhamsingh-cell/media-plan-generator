"""Google Cloud Vision API integration for OCR and image analysis.

Uses the same service-account credentials as Sheets/Slides
(GOOGLE_SLIDES_CREDENTIALS_B64).  Free tier: 1,000 units/month.
"""

from __future__ import annotations

import base64
import json
import logging
import ssl
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_VISION_ENDPOINT = "https://vision.googleapis.com/v1/images:annotate"
_MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10 MB
_MAX_PDF_BYTES = 20 * 1024 * 1024  # 20 MB

# Thread-safe usage tracking (resets on restart)
_usage_lock = threading.Lock()
_usage: Dict[str, Any] = {
    "total_requests": 0,
    "text_detection": 0,
    "label_detection": 0,
    "errors": 0,
    "last_request_at": None,
}


def _get_access_token() -> Optional[str]:
    """Obtain OAuth2 token via shared credential chain in sheets_export."""
    try:
        from sheets_export import _get_access_token as _sheets_token

        return _sheets_token()
    except ImportError:
        logger.error("sheets_export not available -- cannot obtain token")
        return None
    except Exception as exc:
        logger.error("Failed to obtain access token: %s", exc, exc_info=True)
        return None


def _increment_usage(feature: str) -> None:
    with _usage_lock:
        _usage["total_requests"] += 1
        _usage[feature] = _usage.get(feature, 0) + 1
        _usage["last_request_at"] = time.time()


def _increment_errors() -> None:
    with _usage_lock:
        _usage["errors"] += 1


def _vision_request(
    image_content_b64: str,
    features: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Send an annotate request to the Vision API.  Returns first response or None."""
    token = _get_access_token()
    if not token:
        logger.warning("Vision API: no access token -- not configured")
        return None

    body = {
        "requests": [{"image": {"content": image_content_b64}, "features": features}]
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        _VISION_ENDPOINT,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        responses = result.get("responses") or []
        if not responses:
            logger.warning("Vision API returned empty responses array")
            return None
        first = responses[0]
        error = first.get("error")
        if error:
            logger.error(
                "Vision API error: code=%s message=%s",
                error.get("code"),
                error.get("message") or "",
            )
            _increment_errors()
            return None
        return first
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("Vision API HTTP %d: %s", exc.code, error_body, exc_info=True)
        _increment_errors()
        return None
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("Vision API request failed: %s", exc, exc_info=True)
        _increment_errors()
        return None


def extract_text_from_image(image_bytes: bytes, filename: str) -> str:
    """OCR text extraction from an uploaded image (JPEG/PNG/GIF/BMP/WEBP/TIFF).

    Returns extracted text or empty string on failure.
    """
    if not image_bytes:
        return ""
    if len(image_bytes) > _MAX_IMAGE_BYTES:
        logger.warning(
            "Image %s exceeds 10 MB (%d bytes) -- skipping OCR",
            filename,
            len(image_bytes),
        )
        return ""

    _increment_usage("text_detection")
    image_b64 = base64.b64encode(image_bytes).decode("ascii")
    result = _vision_request(image_b64, [{"type": "TEXT_DETECTION", "maxResults": 1}])
    if not result:
        return ""

    # Prefer fullTextAnnotation (best aggregated text)
    full_text = (result.get("fullTextAnnotation") or {}).get("text") or ""
    if full_text:
        logger.info("Vision OCR: %d chars from %s", len(full_text), filename)
        return full_text.strip()

    # Fallback: first textAnnotation
    annotations = result.get("textAnnotations") or []
    if annotations:
        text = annotations[0].get("description") or ""
        logger.info("Vision OCR fallback: %d chars from %s", len(text), filename)
        return text.strip()

    logger.info("Vision OCR found no text in %s", filename)
    return ""


def extract_text_from_pdf_vision(pdf_bytes: bytes, filename: str) -> str:
    """Extract text from a PDF via Vision API (backup for pypdf).

    Uses inputConfig with mimeType=application/pdf, limited to 5 pages / 20 MB.
    Returns extracted text or empty string on failure.
    """
    if not pdf_bytes:
        return ""
    if len(pdf_bytes) > _MAX_PDF_BYTES:
        logger.warning(
            "PDF %s exceeds 20 MB (%d bytes) -- skipping", filename, len(pdf_bytes)
        )
        return ""

    token = _get_access_token()
    if not token:
        return ""

    _increment_usage("text_detection")
    pdf_b64 = base64.b64encode(pdf_bytes).decode("ascii")
    body = {
        "requests": [
            {
                "inputConfig": {"mimeType": "application/pdf", "content": pdf_b64},
                "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
                "pages": [1, 2, 3, 4, 5],
            }
        ]
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        _VISION_ENDPOINT,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        texts: list[str] = []
        for page_resp in result.get("responses") or []:
            err = page_resp.get("error")
            if err:
                logger.warning("Vision PDF page error: %s", err.get("message") or "")
                continue
            page_text = (page_resp.get("fullTextAnnotation") or {}).get("text") or ""
            if page_text:
                texts.append(page_text.strip())
        combined = "\n\n".join(texts)
        if combined:
            logger.info("Vision PDF OCR: %d chars from %s", len(combined), filename)
        else:
            logger.info("Vision PDF OCR found no text in %s", filename)
        return combined
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("Vision PDF HTTP %d: %s", exc.code, error_body, exc_info=True)
        _increment_errors()
        return ""
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("Vision PDF API failed: %s", exc, exc_info=True)
        _increment_errors()
        return ""


def detect_labels(image_bytes: bytes) -> list[dict]:
    """Detect labels (objects, scenes, concepts) in an image.

    Returns list of dicts with 'description', 'score', 'mid' keys.
    """
    if not image_bytes:
        return []
    if len(image_bytes) > _MAX_IMAGE_BYTES:
        logger.warning("Image exceeds 10 MB -- skipping label detection")
        return []

    _increment_usage("label_detection")
    image_b64 = base64.b64encode(image_bytes).decode("ascii")
    result = _vision_request(image_b64, [{"type": "LABEL_DETECTION", "maxResults": 10}])
    if not result:
        return []

    labels = []
    for ann in result.get("labelAnnotations") or []:
        labels.append(
            {
                "description": ann.get("description") or "",
                "score": round(ann.get("score") or 0.0, 4),
                "mid": ann.get("mid") or "",
            }
        )
    logger.info("Vision detected %d labels", len(labels))
    return labels


def get_status() -> dict:
    """Health check -- returns config state and usage counters."""
    configured = _get_access_token() is not None
    with _usage_lock:
        usage_snapshot = dict(_usage)
    return {
        "configured": configured,
        "endpoint": _VISION_ENDPOINT,
        "max_image_bytes": _MAX_IMAGE_BYTES,
        "free_tier_limit": 1000,
        "usage": usage_snapshot,
    }
