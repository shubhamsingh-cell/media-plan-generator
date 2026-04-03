"""Google reCAPTCHA Enterprise + Cloud Text-to-Speech integration.

Reuses credential/token helpers from sheets_export.py
(shared GOOGLE_SLIDES_CREDENTIALS_B64 env var).
reCAPTCHA Enterprise: 1M assessments/month free.
Cloud Text-to-Speech: 4M characters/month free (Neural2 voices).
"""

from __future__ import annotations

import base64, json, logging, os, ssl, threading, time
import urllib.error, urllib.parse, urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_GCP_PROJECT = os.environ.get("GCP_PROJECT_ID") or "gen-lang-client-0603536849"
_RECAPTCHA_SITE_KEY = os.environ.get("RECAPTCHA_SITE_KEY") or ""
_RECAPTCHA_BASE = (
    f"https://recaptchaenterprise.googleapis.com/v1/"
    f"projects/{_GCP_PROJECT}/assessments"
)
_TTS_SYNTHESIZE = "https://texttospeech.googleapis.com/v1/text:synthesize"
_TTS_VOICES = "https://texttospeech.googleapis.com/v1/voices"

_usage_lock = threading.Lock()
_usage: Dict[str, Any] = {
    "recaptcha_assessments": 0,
    "tts_requests": 0,
    "tts_chars": 0,
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


def _incr(field: str, amount: int = 1) -> None:
    with _usage_lock:
        _usage[field] = _usage.get(field, 0) + amount
        _usage["last_request_at"] = time.time()


def _api_request(
    method: str,
    url: str,
    body: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 15,
) -> Optional[bytes]:
    """Authenticated request to a Google Cloud REST API."""
    token = _get_access_token()
    if not token:
        logger.warning("Google API not configured -- no access token")
        return None
    all_h: Dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if headers:
        all_h.update(headers)
    req = urllib.request.Request(url, data=body, headers=all_h, method=method)
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
        logger.error(
            "Google API %s %s -> %d: %s", method, url, exc.code, err, exc_info=True
        )
    except urllib.error.URLError as exc:
        logger.error("Google API request failed: %s", exc, exc_info=True)
    with _usage_lock:
        _usage["errors"] += 1
    return None


# -- reCAPTCHA Enterprise ---------------------------------------------------


def get_recaptcha_site_key() -> str:
    """Return configured reCAPTCHA site key from RECAPTCHA_SITE_KEY env var."""
    return _RECAPTCHA_SITE_KEY


def assess_risk(token: str, action: str, site_key: str = "") -> Dict[str, Any]:
    """Score a user action via reCAPTCHA Enterprise.

    Returns dict: score (0.0=bot, 1.0=human), reasons, valid, raw.
    """
    result: Dict[str, Any] = {"score": 0.0, "reasons": [], "valid": False, "raw": None}
    if not token:
        result["reasons"] = ["missing_token"]
        return result
    effective_key = site_key or _RECAPTCHA_SITE_KEY
    if not effective_key:
        result["reasons"] = ["missing_site_key"]
        logger.warning("reCAPTCHA site key not configured")
        return result

    payload = json.dumps(
        {"event": {"token": token, "siteKey": effective_key, "expectedAction": action}}
    ).encode("utf-8")

    raw = _api_request("POST", _RECAPTCHA_BASE, body=payload)
    if raw is None:
        result["reasons"] = ["api_error"]
        return result
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        result["reasons"] = ["invalid_response"]
        return result

    _incr("recaptcha_assessments")
    result["raw"] = data
    token_props = data.get("tokenProperties") or {}
    risk = data.get("riskAnalysis") or {}
    result["valid"] = token_props.get("valid", False)
    result["score"] = risk.get("score", 0.0)
    result["reasons"] = risk.get("reasons") or []
    if not result["valid"]:
        result["reasons"] = result["reasons"] + [
            token_props.get("invalidReason") or "INVALID"
        ]
    return result


def is_bot(token: str, action: str, threshold: float = 0.5) -> bool:
    """Return True if likely a bot (score < threshold or invalid token)."""
    assessment = assess_risk(token, action)
    if not assessment["valid"]:
        return True
    return assessment["score"] < threshold


# -- Cloud Text-to-Speech ---------------------------------------------------


def text_to_speech(
    text: str,
    language: str = "en-US",
    voice_name: str = "en-US-Neural2-C",
) -> bytes:
    """Convert text to MP3 audio bytes. Returns empty bytes on failure."""
    if not text:
        return b""
    text = text[:5000]  # API max per request
    payload = json.dumps(
        {
            "input": {"text": text},
            "voice": {"languageCode": language, "name": voice_name},
            "audioConfig": {"audioEncoding": "MP3", "speakingRate": 1.0, "pitch": 0.0},
        }
    ).encode("utf-8")

    raw = _api_request("POST", _TTS_SYNTHESIZE, body=payload, timeout=30)
    if raw is None:
        return b""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.error("TTS returned non-JSON response")
        return b""
    audio_b64 = data.get("audioContent") or ""
    if not audio_b64:
        logger.warning("TTS response missing audioContent")
        return b""
    _incr("tts_requests")
    _incr("tts_chars", len(text))
    try:
        return base64.b64decode(audio_b64)
    except Exception as exc:
        logger.error("Failed to decode TTS audio: %s", exc, exc_info=True)
        return b""


def synthesize_plan_summary(plan_data: dict) -> bytes:
    """Generate audio summary of a media plan dict. Returns MP3 bytes."""
    if not plan_data:
        return b""
    title = plan_data.get("title") or plan_data.get("name") or "Untitled Plan"
    budget = plan_data.get("total_budget") or plan_data.get("budget") or "not specified"
    channels = plan_data.get("channels") or plan_data.get("channel_mix") or []
    duration = (
        plan_data.get("duration") or plan_data.get("flight_dates") or "not specified"
    )
    objective = plan_data.get("objective") or plan_data.get("goal") or ""

    channel_text = ""
    if isinstance(channels, list) and channels:
        names = []
        for ch in channels[:5]:
            if isinstance(ch, dict):
                names.append(ch.get("name") or ch.get("channel") or "unknown")
            elif isinstance(ch, str):
                names.append(ch)
        if names:
            channel_text = f" The recommended channels are {', '.join(names)}."

    parts = [f"Here is the summary for {title}."]
    if objective:
        parts.append(f"The campaign objective is {objective}.")
    parts.append(f"The total budget is {budget}.")
    if duration:
        parts.append(f"The campaign duration is {duration}.")
    if channel_text:
        parts.append(channel_text)
    return text_to_speech(" ".join(parts))


def list_voices(language: str = "en") -> List[Dict[str, Any]]:
    """List available TTS voices filtered by language prefix."""
    url = _TTS_VOICES
    if language:
        url = f"{_TTS_VOICES}?languageCode={urllib.parse.quote(language)}"
    raw = _api_request("GET", url)
    if raw is None:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.error("TTS voices returned non-JSON response")
        return []
    return [
        {
            "name": v.get("name") or "",
            "language_codes": v.get("languageCodes") or [],
            "gender": v.get("ssmlGender") or "UNSPECIFIED",
            "sample_rate_hz": v.get("naturalSampleRateHertz") or 0,
        }
        for v in (data.get("voices") or [])
    ]


# -- Health / Status --------------------------------------------------------


def get_status() -> Dict[str, Any]:
    """Health check. Returns config state, usage counters, and free-tier info."""
    has_creds = _get_access_token() is not None
    with _usage_lock:
        snap = dict(_usage)
    return {
        "service": "google_security_tts",
        "configured": has_creds,
        "recaptcha": {
            "site_key_set": bool(_RECAPTCHA_SITE_KEY),
            "gcp_project": _GCP_PROJECT,
            "assessments_used": snap.get("recaptcha_assessments", 0),
            "free_tier": "1M assessments/month",
        },
        "tts": {
            "requests": snap.get("tts_requests", 0),
            "chars_used": snap.get("tts_chars", 0),
            "free_tier": "4M chars/month (Neural2 voices)",
        },
        "errors": snap.get("errors", 0),
        "last_request_at": snap.get("last_request_at"),
    }
