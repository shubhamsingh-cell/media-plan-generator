"""Google Cloud Translation API v2 Basic integration.

Reuses _get_access_token from sheets_export.py for service-account auth.
Env: GOOGLE_SLIDES_CREDENTIALS_B64 (base64-encoded service account JSON).
Free tier: 500K chars/month.
"""

from __future__ import annotations

import json
import logging
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_TRANSLATE_BASE = "https://translation.googleapis.com/language/translate/v2"
_FREE_CHAR_LIMIT = 500_000

# Thread-safe character usage counter (resets monthly)
_usage_lock = threading.Lock()
_usage: Dict[str, Any] = {
    "chars": 0,
    "month": datetime.now(timezone.utc).strftime("%Y-%m"),
}


def _track_chars(count: int) -> None:
    """Add *count* to the monthly character usage counter."""
    with _usage_lock:
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        if _usage["month"] != month:
            _usage["chars"] = 0
            _usage["month"] = month
        _usage["chars"] += count


def _get_char_usage() -> Dict[str, Any]:
    """Return a snapshot of character usage."""
    with _usage_lock:
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        if _usage["month"] != month:
            _usage["chars"] = 0
            _usage["month"] = month
        return {
            "month": _usage["month"],
            "chars_used": _usage["chars"],
            "chars_remaining": max(0, _FREE_CHAR_LIMIT - _usage["chars"]),
            "limit": _FREE_CHAR_LIMIT,
        }


def _api_request(
    url: str, *, method: str = "GET", body: Optional[dict] = None, token: str
) -> dict:
    """Authenticated request to the Cloud Translation API."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        logger.error(
            "Translation API %s %s failed (%s): %s",
            method,
            url,
            exc.code,
            err,
            exc_info=True,
        )
        raise
    except urllib.error.URLError as exc:
        logger.error("Translation API network error: %s", exc, exc_info=True)
        raise


def _get_token() -> str:
    """Obtain an access token via sheets_export, raising on failure."""
    from sheets_export import _get_access_token

    token = _get_access_token()
    if not token:
        raise RuntimeError("Google Cloud credentials not configured")
    return token


# -- Public API ---------------------------------------------------------------


def translate_text(
    text: str, target_lang: str, source_lang: str = ""
) -> Dict[str, str]:
    """Translate a single text string.

    Returns: {"translated_text": str, "detected_source_lang": str}
    """
    token = _get_token()
    payload: Dict[str, Any] = {"q": text, "target": target_lang, "format": "text"}
    if source_lang:
        payload["source"] = source_lang
    result = _api_request(_TRANSLATE_BASE, method="POST", body=payload, token=token)
    t = result["data"]["translations"][0]
    _track_chars(len(text))
    return {
        "translated_text": t["translatedText"],
        "detected_source_lang": t.get("detectedSourceLanguage") or source_lang,
    }


def detect_language(text: str) -> Dict[str, Any]:
    """Detect the language of *text*.

    Returns: {"language": str, "confidence": float}
    """
    token = _get_token()
    result = _api_request(
        f"{_TRANSLATE_BASE}/detect", method="POST", body={"q": text}, token=token
    )
    det = result["data"]["detections"][0][0]
    _track_chars(len(text))
    return {
        "language": det["language"],
        "confidence": float(det.get("confidence", 0.0)),
    }


def translate_job_posting(
    title: str, description: str, target_lang: str
) -> Dict[str, str]:
    """Translate a job title and description in a single API call.

    Returns: {"title": str, "description": str, "source_lang": str}
    """
    token = _get_token()
    payload: Dict[str, Any] = {
        "q": [title, description],
        "target": target_lang,
        "format": "text",
    }
    result = _api_request(_TRANSLATE_BASE, method="POST", body=payload, token=token)
    translations = result["data"]["translations"]
    _track_chars(len(title) + len(description))
    return {
        "title": translations[0]["translatedText"],
        "description": translations[1]["translatedText"],
        "source_lang": translations[0].get("detectedSourceLanguage") or "",
    }


def batch_translate(
    texts: list[str], target_lang: str, *, chunk_size: int = 100
) -> list[Dict[str, str]]:
    """Translate a list of texts with chunking and rate-limit backoff.

    Returns: list of {"translated_text": str, "detected_source_lang": str}
    """
    token = _get_token()
    results: list[Dict[str, str]] = []
    for i in range(0, len(texts), chunk_size):
        chunk = texts[i : i + chunk_size]
        payload: Dict[str, Any] = {"q": chunk, "target": target_lang, "format": "text"}
        try:
            resp = _api_request(
                _TRANSLATE_BASE, method="POST", body=payload, token=token
            )
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                logger.warning("Rate limited, backing off 5 s")
                time.sleep(5)
                resp = _api_request(
                    _TRANSLATE_BASE, method="POST", body=payload, token=token
                )
            else:
                raise
        for t in resp["data"]["translations"]:
            results.append(
                {
                    "translated_text": t["translatedText"],
                    "detected_source_lang": t.get("detectedSourceLanguage") or "",
                }
            )
        _track_chars(sum(len(t) for t in chunk))
        if i + chunk_size < len(texts):
            time.sleep(0.1)
    return results


def get_supported_languages() -> list[Dict[str, str]]:
    """Return languages supported by the Translation API.

    Returns: list of {"language": str, "name": str}
    """
    token = _get_token()
    result = _api_request(
        f"{_TRANSLATE_BASE}/languages?target=en", method="GET", body=None, token=token
    )
    return [
        {"language": lang["language"], "name": lang.get("name") or lang["language"]}
        for lang in result["data"]["languages"]
    ]


def get_status() -> Dict[str, Any]:
    """Health check with character usage tracking.

    Returns: {"ok": bool, "error": str|None, "usage": dict}
    """
    usage = _get_char_usage()
    try:
        token = _get_token()
        url = f"{_TRANSLATE_BASE}/languages?target=en"
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {token}"}, method="GET"
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            resp.read()
        return {"ok": True, "error": None, "usage": usage}
    except Exception as exc:
        logger.error("Translation API health check failed: %s", exc, exc_info=True)
        return {"ok": False, "error": str(exc), "usage": usage}
