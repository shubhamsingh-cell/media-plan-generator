"""ElevenLabs API Integration for Nova AI Suite.

Provides text-to-speech, speech-to-text, sound effects, voice design,
ad voiceover generation, and audio summary capabilities using ElevenLabs APIs.

All HTTP calls use urllib.request (stdlib only, no third-party deps).
Thread-safe with credit tracking and in-memory caching.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Iterator, Optional

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────

ELEVENLABS_API_KEY: str = os.environ.get("ELEVENLABS_API_KEY") or ""
ELEVENLABS_BASE_URL: str = "https://api.elevenlabs.io/v1"

# Default voice: George (warm, professional)
DEFAULT_VOICE_ID: str = "JBFqnCBsd6RMkjVDRZzb"
DEFAULT_TTS_MODEL: str = "eleven_flash_v2_5"
DEFAULT_OUTPUT_FORMAT: str = "mp3_44100_128"

# Voice mapping for ad voiceover tones
TONE_VOICE_MAP: Dict[str, str] = {
    "professional": "JBFqnCBsd6RMkjVDRZzb",  # George
    "friendly": "EXAVITQu4vr4xnSDxMaL",  # Bella
    "authoritative": "VR6AewLTigWG4xSOukaG",  # Arnold
    "energetic": "pNInz6obpgDQGcFmaJgB",  # Adam
    "warm": "21m00Tcm4TlvDq8ikWAM",  # Rachel
}

# ─── Rate Limiting & Concurrency ─────────────────────────────────────────────

_credit_lock = threading.Lock()
_credit_usage: Dict[str, Any] = {
    "total_characters_used": 0,
    "total_requests": 0,
    "last_reset": time.time(),
}

_concurrency_semaphore = threading.Semaphore(10)

# ─── In-Memory TTS Cache ─────────────────────────────────────────────────────

_tts_cache_lock = threading.Lock()
_tts_cache: Dict[str, bytes] = {}
_TTS_CACHE_MAX_SIZE: int = 50


def _cache_key(text: str, voice_id: str, model_id: str) -> str:
    """Generate a deterministic cache key for TTS requests."""
    raw = f"{text}|{voice_id}|{model_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _get_cached_tts(key: str) -> Optional[bytes]:
    """Retrieve cached TTS audio if available."""
    with _tts_cache_lock:
        return _tts_cache.get(key)


def _set_cached_tts(key: str, audio: bytes) -> None:
    """Store TTS audio in cache, evicting oldest if full."""
    with _tts_cache_lock:
        if len(_tts_cache) >= _TTS_CACHE_MAX_SIZE:
            # Evict first inserted key (FIFO)
            oldest_key = next(iter(_tts_cache))
            del _tts_cache[oldest_key]
        _tts_cache[key] = audio


def _track_usage(characters: int) -> None:
    """Thread-safe credit usage tracking."""
    with _credit_lock:
        _credit_usage["total_characters_used"] += characters
        _credit_usage["total_requests"] += 1


# ─── HTTP Helpers ─────────────────────────────────────────────────────────────


def _make_request(
    endpoint: str,
    method: str = "GET",
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
) -> Optional[bytes]:
    """Make an HTTP request to the ElevenLabs API.

    Args:
        endpoint: API endpoint path (e.g., '/text-to-speech/{voice_id}').
        method: HTTP method ('GET' or 'POST').
        data: Request body bytes.
        headers: Additional HTTP headers.
        timeout: Request timeout in seconds.

    Returns:
        Response body bytes, or None on failure.
    """
    if not ELEVENLABS_API_KEY:
        logger.error("ELEVENLABS_API_KEY is not set")
        return None

    url = f"{ELEVENLABS_BASE_URL}{endpoint}"
    req_headers: Dict[str, str] = {
        "xi-api-key": ELEVENLABS_API_KEY,
    }
    if headers:
        req_headers.update(headers)

    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)

    acquired = _concurrency_semaphore.acquire(timeout=10)
    if not acquired:
        logger.error("ElevenLabs concurrency limit reached (max 10)")
        return None

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read()
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        if e.code == 401:
            logger.warning(
                "ElevenLabs API key is invalid or expired (HTTP 401) for %s. "
                "Check ELEVENLABS_API_KEY env var on Render.",
                endpoint,
            )
        else:
            logger.error(
                f"ElevenLabs API HTTP {e.code} for {endpoint}: {error_body}",
                exc_info=True,
            )
        return None
    except urllib.error.URLError as e:
        logger.warning(f"ElevenLabs API connection error for {endpoint}: {e}")
        return None
    except OSError as e:
        logger.warning(f"ElevenLabs API OS error for {endpoint}: {e}")
        return None
    finally:
        _concurrency_semaphore.release()


def _make_multipart_request(
    endpoint: str,
    fields: Dict[str, str],
    file_field: str,
    file_data: bytes,
    file_name: str,
    file_content_type: str = "application/octet-stream",
    timeout: int = 60,
) -> Optional[bytes]:
    """Make a multipart/form-data POST request to the ElevenLabs API.

    Args:
        endpoint: API endpoint path.
        fields: Form fields as key-value pairs.
        file_field: Name of the file field.
        file_data: File content bytes.
        file_name: Filename for the upload.
        file_content_type: MIME type of the file.
        timeout: Request timeout in seconds.

    Returns:
        Response body bytes, or None on failure.
    """
    if not ELEVENLABS_API_KEY:
        logger.error("ELEVENLABS_API_KEY is not set")
        return None

    boundary = f"----NovaElevenLabs{int(time.time() * 1000)}"
    body_parts: list[bytes] = []

    for key, value in fields.items():
        body_parts.append(f"--{boundary}\r\n".encode())
        body_parts.append(
            f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode()
        )
        body_parts.append(f"{value}\r\n".encode())

    body_parts.append(f"--{boundary}\r\n".encode())
    body_parts.append(
        f'Content-Disposition: form-data; name="{file_field}"; '
        f'filename="{file_name}"\r\n'.encode()
    )
    body_parts.append(f"Content-Type: {file_content_type}\r\n\r\n".encode())
    body_parts.append(file_data)
    body_parts.append(f"\r\n--{boundary}--\r\n".encode())

    body = b"".join(body_parts)

    url = f"{ELEVENLABS_BASE_URL}{endpoint}"
    req_headers = {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    }
    req = urllib.request.Request(url, data=body, headers=req_headers, method="POST")

    acquired = _concurrency_semaphore.acquire(timeout=10)
    if not acquired:
        logger.error("ElevenLabs concurrency limit reached (max 10)")
        return None

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read()
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        if e.code == 401:
            logger.warning(
                "ElevenLabs API key is invalid or expired (HTTP 401) for %s. "
                "Check ELEVENLABS_API_KEY env var on Render.",
                endpoint,
            )
        else:
            logger.error(
                f"ElevenLabs multipart HTTP {e.code} for {endpoint}: {error_body}",
                exc_info=True,
            )
        return None
    except urllib.error.URLError as e:
        logger.error(
            f"ElevenLabs multipart connection error for {endpoint}: {e}",
            exc_info=True,
        )
        return None
    except OSError as e:
        logger.error(
            f"ElevenLabs multipart OS error for {endpoint}: {e}",
            exc_info=True,
        )
        return None
    finally:
        _concurrency_semaphore.release()


# ═════════════════════════════════════════════════════════════════════════════
# 1. TEXT-TO-SPEECH (TTS)
# ═════════════════════════════════════════════════════════════════════════════


def text_to_speech(
    text: str,
    voice_id: Optional[str] = None,
    model_id: str = DEFAULT_TTS_MODEL,
) -> Optional[bytes]:
    """Convert text to speech audio using ElevenLabs TTS API.

    Args:
        text: The text to convert to speech (max 5000 characters).
        voice_id: ElevenLabs voice ID. Defaults to George (warm, professional).
        model_id: TTS model ID. Defaults to eleven_flash_v2_5.

    Returns:
        MP3 audio bytes, or None on failure.
    """
    if not text or not text.strip():
        logger.warning("Empty text passed to text_to_speech")
        return None

    voice = voice_id or DEFAULT_VOICE_ID
    ck = _cache_key(text, voice, model_id)

    cached = _get_cached_tts(ck)
    if cached is not None:
        logger.info("TTS cache hit for voice=%s, text_len=%d", voice, len(text))
        return cached

    payload = json.dumps(
        {
            "text": text[:5000],
            "model_id": model_id,
            "voice_settings": {
                "stability": 0.5,
                "similarity_boost": 0.75,
                "style": 0.0,
                "use_speaker_boost": True,
            },
        }
    ).encode("utf-8")

    audio = _make_request(
        f"/text-to-speech/{voice}?output_format={DEFAULT_OUTPUT_FORMAT}",
        method="POST",
        data=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )

    if audio:
        _track_usage(len(text))
        _set_cached_tts(ck, audio)
        logger.info(
            "TTS generated: voice=%s, text_len=%d, audio_bytes=%d",
            voice,
            len(text),
            len(audio),
        )

    return audio


def text_to_speech_stream(
    text: str,
    voice_id: Optional[str] = None,
    model_id: str = DEFAULT_TTS_MODEL,
) -> Iterator[bytes]:
    """Stream text-to-speech audio in chunks using ElevenLabs streaming API.

    Args:
        text: The text to convert to speech.
        voice_id: ElevenLabs voice ID. Defaults to George.
        model_id: TTS model ID. Defaults to eleven_flash_v2_5.

    Yields:
        Chunks of MP3 audio bytes.
    """
    if not text or not text.strip():
        logger.warning("Empty text passed to text_to_speech_stream")
        return

    if not ELEVENLABS_API_KEY:
        logger.error("ELEVENLABS_API_KEY is not set")
        return

    voice = voice_id or DEFAULT_VOICE_ID

    payload = json.dumps(
        {
            "text": text[:5000],
            "model_id": model_id,
            "voice_settings": {
                "stability": 0.5,
                "similarity_boost": 0.75,
                "style": 0.0,
                "use_speaker_boost": True,
            },
        }
    ).encode("utf-8")

    url = (
        f"{ELEVENLABS_BASE_URL}/text-to-speech/{voice}/stream"
        f"?output_format={DEFAULT_OUTPUT_FORMAT}"
    )
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "xi-api-key": ELEVENLABS_API_KEY,
            "Content-Type": "application/json",
        },
        method="POST",
    )

    acquired = _concurrency_semaphore.acquire(timeout=10)
    if not acquired:
        logger.error("ElevenLabs concurrency limit reached for stream")
        return

    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            _track_usage(len(text))
            while True:
                chunk = response.read(4096)
                if not chunk:
                    break
                yield chunk
    except urllib.error.HTTPError as e:
        if e.code == 401:
            logger.warning(
                "ElevenLabs API key is invalid or expired (HTTP 401) for streaming. "
                "Check ELEVENLABS_API_KEY env var on Render.",
            )
        else:
            logger.warning(f"ElevenLabs stream HTTP {e.code}: {e}")
    except urllib.error.URLError as e:
        logger.warning(f"ElevenLabs stream connection error: {e}")
    except OSError as e:
        logger.warning(f"ElevenLabs stream OS error: {e}")
    finally:
        _concurrency_semaphore.release()


# ═════════════════════════════════════════════════════════════════════════════
# 2. SPEECH-TO-TEXT (STT)
# ═════════════════════════════════════════════════════════════════════════════


def speech_to_text(
    audio_data: bytes,
    language: str = "en",
) -> Optional[str]:
    """Transcribe audio to text using ElevenLabs Scribe v2 model.

    Args:
        audio_data: Raw audio bytes (MP3, WAV, etc.).
        language: Language code (e.g., 'en', 'es', 'fr'). Defaults to 'en'.

    Returns:
        Transcribed text string, or None on failure.
    """
    if not audio_data:
        logger.warning("Empty audio_data passed to speech_to_text")
        return None

    fields = {
        "model_id": "scribe_v2",
        "language_code": language,
    }

    response_bytes = _make_multipart_request(
        endpoint="/speech-to-text",
        fields=fields,
        file_field="file",
        file_data=audio_data,
        file_name="audio.mp3",
        file_content_type="audio/mpeg",
        timeout=60,
    )

    if not response_bytes:
        return None

    try:
        result = json.loads(response_bytes.decode("utf-8"))
        transcript = result.get("text") or ""
        logger.info(
            "STT transcribed: language=%s, audio_bytes=%d, text_len=%d",
            language,
            len(audio_data),
            len(transcript),
        )
        return transcript
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning(f"Failed to parse STT response: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════════════
# 3. SOUND EFFECTS
# ═════════════════════════════════════════════════════════════════════════════


def generate_sound_effect(
    description: str,
    duration_seconds: float = 5.0,
) -> Optional[bytes]:
    """Generate a sound effect from a text description.

    Args:
        description: Natural language description of the sound effect.
        duration_seconds: Duration of the generated audio in seconds (0.5-22.0).

    Returns:
        Audio bytes (MP3), or None on failure.
    """
    if not description or not description.strip():
        logger.warning("Empty description passed to generate_sound_effect")
        return None

    clamped_duration = max(0.5, min(22.0, duration_seconds))

    payload = json.dumps(
        {
            "text": description[:500],
            "duration_seconds": clamped_duration,
        }
    ).encode("utf-8")

    audio = _make_request(
        "/sound-effects/generate",
        method="POST",
        data=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )

    if audio:
        _track_usage(len(description))
        logger.info(
            "Sound effect generated: description_len=%d, duration=%.1fs, audio_bytes=%d",
            len(description),
            clamped_duration,
            len(audio),
        )

    return audio


# ═════════════════════════════════════════════════════════════════════════════
# 4. VOICE DESIGN
# ═════════════════════════════════════════════════════════════════════════════


def design_voice(
    description: str,
    sample_text: str = "Hello, I'm Nova, your recruitment intelligence assistant.",
) -> Optional[Dict[str, Any]]:
    """Design a new voice from a text description using ElevenLabs Voice Design.

    Args:
        description: Natural language description of the desired voice
                     (e.g., 'A warm, professional female voice with slight British accent').
        sample_text: Text to generate a preview sample with.

    Returns:
        Dict with 'audio' (base64-encoded preview), 'voice_id', and metadata,
        or None on failure.
    """
    if not description or not description.strip():
        logger.warning("Empty description passed to design_voice")
        return None

    payload = json.dumps(
        {
            "voice_description": description[:500],
            "text": sample_text[:1000],
        }
    ).encode("utf-8")

    response_bytes = _make_request(
        "/text-to-voice/design",
        method="POST",
        data=payload,
        headers={"Content-Type": "application/json"},
        timeout=60,
    )

    if not response_bytes:
        return None

    try:
        result = json.loads(response_bytes.decode("utf-8"))
        logger.info("Voice designed: description_len=%d", len(description))
        return result
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning(f"Failed to parse voice design response: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════════════
# 5. VOICEOVER FOR ADS (CreativeAI)
# ═════════════════════════════════════════════════════════════════════════════


def generate_ad_voiceover(
    ad_copy: str,
    tone: str = "professional",
) -> Optional[bytes]:
    """Generate a voiceover for advertising copy using ElevenLabs.

    Uses eleven_v3 model for maximum expressiveness in ad content.
    Automatically selects a voice based on the requested tone.

    Args:
        ad_copy: The advertising copy text to narrate.
        tone: Desired tone. One of: professional, friendly, authoritative,
              energetic, warm. Defaults to 'professional'.

    Returns:
        MP3 audio bytes, or None on failure.
    """
    if not ad_copy or not ad_copy.strip():
        logger.warning("Empty ad_copy passed to generate_ad_voiceover")
        return None

    voice_id = TONE_VOICE_MAP.get(tone) or TONE_VOICE_MAP["professional"]

    payload = json.dumps(
        {
            "text": ad_copy[:5000],
            "model_id": "eleven_v3",
            "voice_settings": {
                "stability": 0.4,
                "similarity_boost": 0.8,
                "style": 0.6,
                "use_speaker_boost": True,
            },
        }
    ).encode("utf-8")

    audio = _make_request(
        f"/text-to-speech/{voice_id}?output_format={DEFAULT_OUTPUT_FORMAT}",
        method="POST",
        data=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )

    if audio:
        _track_usage(len(ad_copy))
        logger.info(
            "Ad voiceover generated: tone=%s, voice=%s, text_len=%d, audio_bytes=%d",
            tone,
            voice_id,
            len(ad_copy),
            len(audio),
        )

    return audio


# ═════════════════════════════════════════════════════════════════════════════
# 6. AUDIO SUMMARY (Media Plan)
# ═════════════════════════════════════════════════════════════════════════════


def _format_plan_narration(plan_data: Dict[str, Any]) -> str:
    """Format a media plan dictionary into a narration script.

    Args:
        plan_data: Media plan data dictionary.

    Returns:
        Formatted narration text string.
    """
    company = plan_data.get("company_name") or plan_data.get("company") or "the client"
    role = plan_data.get("role") or plan_data.get("job_title") or "the open position"
    budget = plan_data.get("budget") or plan_data.get("total_budget") or "not specified"
    location = plan_data.get("location") or "multiple locations"

    sections: list[str] = [
        f"Here is your media plan summary for {company}.",
        f"The role being recruited is {role}, based in {location}.",
        f"The total budget allocated is {budget}.",
    ]

    channels = plan_data.get("channels") or plan_data.get("recommended_channels") or []
    if channels:
        if isinstance(channels, list):
            channel_names: list[str] = []
            for ch in channels[:5]:
                if isinstance(ch, dict):
                    channel_names.append(ch.get("name") or ch.get("channel") or "")
                elif isinstance(ch, str):
                    channel_names.append(ch)
            named = [c for c in channel_names if c]
            if named:
                sections.append(f"The recommended channels are: {', '.join(named)}.")

    timeline = plan_data.get("timeline") or plan_data.get("duration") or ""
    if timeline:
        sections.append(f"The campaign timeline is {timeline}.")

    kpis = plan_data.get("kpis") or plan_data.get("expected_results") or {}
    if isinstance(kpis, dict) and kpis:
        kpi_parts: list[str] = []
        for k, v in list(kpis.items())[:4]:
            kpi_parts.append(f"{k}: {v}")
        if kpi_parts:
            sections.append(
                f"Key performance indicators include {', '.join(kpi_parts)}."
            )

    sections.append(
        "This concludes your media plan audio summary. "
        "For detailed breakdowns, please refer to the full report."
    )

    return " ".join(sections)


def generate_audio_summary(plan_data: Dict[str, Any]) -> Optional[bytes]:
    """Generate an audio narration of a media plan summary.

    Formats the plan data into a narration script, then converts to speech
    using the default professional voice.

    Args:
        plan_data: Media plan data dictionary containing company, role,
                   budget, channels, timeline, and KPIs.

    Returns:
        MP3 audio bytes of the narrated summary, or None on failure.
    """
    if not plan_data:
        logger.warning("Empty plan_data passed to generate_audio_summary")
        return None

    script = _format_plan_narration(plan_data)
    return text_to_speech(script, voice_id=DEFAULT_VOICE_ID, model_id=DEFAULT_TTS_MODEL)


# ═════════════════════════════════════════════════════════════════════════════
# 7. HEALTH CHECK
# ═════════════════════════════════════════════════════════════════════════════


def check_elevenlabs_health() -> Dict[str, Any]:
    """Check ElevenLabs API health by verifying API key validity and quota.

    Returns:
        Dict with 'healthy' (bool), 'subscription' info, 'usage' stats,
        and 'error' (if any).
    """
    result: Dict[str, Any] = {
        "healthy": False,
        "api_key_configured": bool(ELEVENLABS_API_KEY),
        "usage": {},
        "subscription": {},
        "error": None,
    }

    if not ELEVENLABS_API_KEY:
        result["error"] = "ELEVENLABS_API_KEY is not set"
        return result

    response_bytes = _make_request("/user", method="GET", timeout=10)

    if not response_bytes:
        result["error"] = "Failed to reach ElevenLabs API"
        return result

    try:
        user_data = json.loads(response_bytes.decode("utf-8"))
        subscription = user_data.get("subscription") or {}

        result["healthy"] = True
        result["subscription"] = {
            "tier": subscription.get("tier") or "unknown",
            "character_count": subscription.get("character_count") or 0,
            "character_limit": subscription.get("character_limit") or 0,
            "next_character_count_reset_unix": subscription.get(
                "next_character_count_reset_unix"
            )
            or 0,
        }

        char_count = subscription.get("character_count") or 0
        char_limit = subscription.get("character_limit") or 1
        result["subscription"]["usage_percent"] = round(
            (char_count / char_limit) * 100, 1
        )

        with _credit_lock:
            result["usage"] = {
                "session_characters_used": _credit_usage["total_characters_used"],
                "session_requests": _credit_usage["total_requests"],
                "session_start": _credit_usage["last_reset"],
            }

        logger.info(
            "ElevenLabs health OK: tier=%s, usage=%d/%d",
            subscription.get("tier") or "unknown",
            char_count,
            char_limit,
        )

    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        result["error"] = f"Failed to parse health response: {e}"
        logger.warning(f"ElevenLabs health parse error: {e}")

    return result


# ═════════════════════════════════════════════════════════════════════════════
# 8. RESILIENT TTS WITH FALLBACK
# ═════════════════════════════════════════════════════════════════════════════


# Rate limiting for ElevenLabs API (sliding window)
_elevenlabs_rpm_limit = 20  # Conservative limit
_elevenlabs_request_times: list[float] = []
_elevenlabs_rate_lock = threading.Lock()


def _is_elevenlabs_rate_limited() -> bool:
    """Check if ElevenLabs API is rate limited."""
    now = time.time()
    with _elevenlabs_rate_lock:
        _elevenlabs_request_times[:] = [
            t for t in _elevenlabs_request_times if now - t < 60
        ]
        return len(_elevenlabs_request_times) >= _elevenlabs_rpm_limit


def _record_elevenlabs_request() -> None:
    """Record an ElevenLabs API request for rate limiting."""
    with _elevenlabs_rate_lock:
        _elevenlabs_request_times.append(time.time())


def text_to_speech_with_fallback(
    text: str,
    voice_id: Optional[str] = None,
    model_id: str = DEFAULT_TTS_MODEL,
) -> Dict[str, Any]:
    """Convert text to speech with automatic fallback to client-side TTS.

    Returns a structured response that the frontend can use to either
    play server-generated audio or fall back to the browser's Web Speech API.

    Fallback tiers:
        Tier 1: ElevenLabs server-side TTS (high quality)
        Tier 2: Client-side Web Speech API (browser built-in, free)
        Tier 3: Text-only response (no audio)

    Args:
        text: The text to convert to speech (max 5000 characters).
        voice_id: ElevenLabs voice ID. Defaults to George.
        model_id: TTS model ID. Defaults to eleven_flash_v2_5.

    Returns:
        Dict with keys:
            - type: "audio" | "web_speech" | "text_only"
            - audio: bytes | None (MP3 audio if type="audio")
            - text: str (the original text for fallback rendering)
            - message: str (status message for the UI)
    """
    if not text or not text.strip():
        return {
            "type": "text_only",
            "audio": None,
            "text": "",
            "message": "Empty text provided",
        }

    # Check if ElevenLabs is available before trying
    if not ELEVENLABS_API_KEY:
        logger.info("ElevenLabs not configured, using Web Speech API fallback")
        return {
            "type": "web_speech",
            "audio": None,
            "text": text,
            "message": "Using browser text-to-speech (ElevenLabs not configured)",
        }

    # Check rate limit before making the call
    if _is_elevenlabs_rate_limited():
        logger.warning("ElevenLabs rate limited, using Web Speech API fallback")
        return {
            "type": "web_speech",
            "audio": None,
            "text": text,
            "message": "Using browser text-to-speech (rate limit reached)",
        }

    # Tier 1: Try ElevenLabs server-side TTS
    _record_elevenlabs_request()
    audio = text_to_speech(text, voice_id=voice_id, model_id=model_id)

    if audio is not None:
        return {
            "type": "audio",
            "audio": audio,
            "text": text,
            "message": "Audio generated by ElevenLabs",
        }

    # Tier 2: ElevenLabs failed -- signal client to use Web Speech API
    logger.warning("ElevenLabs TTS failed, falling back to Web Speech API")
    return {
        "type": "web_speech",
        "audio": None,
        "text": text,
        "message": "Using browser text-to-speech (server audio unavailable)",
    }


def generate_ad_voiceover_with_fallback(
    ad_copy: str,
    tone: str = "professional",
) -> Dict[str, Any]:
    """Generate ad voiceover with fallback to client-side TTS.

    Args:
        ad_copy: The advertising copy text to narrate.
        tone: Desired tone (professional, friendly, etc.).

    Returns:
        Dict with type, audio, text, and message keys.
    """
    if not ad_copy or not ad_copy.strip():
        return {
            "type": "text_only",
            "audio": None,
            "text": "",
            "message": "Empty ad copy provided",
        }

    if not ELEVENLABS_API_KEY or _is_elevenlabs_rate_limited():
        return {
            "type": "web_speech",
            "audio": None,
            "text": ad_copy,
            "message": "Using browser text-to-speech for ad voiceover",
        }

    _record_elevenlabs_request()
    audio = generate_ad_voiceover(ad_copy, tone=tone)

    if audio is not None:
        return {
            "type": "audio",
            "audio": audio,
            "text": ad_copy,
            "message": f"Ad voiceover generated (tone: {tone})",
        }

    return {
        "type": "web_speech",
        "audio": None,
        "text": ad_copy,
        "message": "Using browser text-to-speech for ad voiceover (server unavailable)",
    }


def generate_audio_summary_with_fallback(
    plan_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Generate audio summary with fallback.

    Args:
        plan_data: Media plan data dictionary.

    Returns:
        Dict with type, audio, text, and message keys.
    """
    if not plan_data:
        return {
            "type": "text_only",
            "audio": None,
            "text": "",
            "message": "Empty plan data",
        }

    script = _format_plan_narration(plan_data)

    return text_to_speech_with_fallback(
        text=script,
        voice_id=DEFAULT_VOICE_ID,
        model_id=DEFAULT_TTS_MODEL,
    )


# ═════════════════════════════════════════════════════════════════════════════
# 9. UTILITY: CREDIT USAGE STATS
# ═════════════════════════════════════════════════════════════════════════════


def get_usage_stats() -> Dict[str, Any]:
    """Get current session credit usage statistics.

    Returns:
        Dict with total_characters_used, total_requests, and session duration.
    """
    with _credit_lock:
        return {
            "total_characters_used": _credit_usage["total_characters_used"],
            "total_requests": _credit_usage["total_requests"],
            "session_duration_seconds": round(
                time.time() - _credit_usage["last_reset"], 1
            ),
        }


def clear_tts_cache() -> int:
    """Clear the TTS audio cache and return the number of entries removed.

    Returns:
        Number of cache entries that were cleared.
    """
    with _tts_cache_lock:
        count = len(_tts_cache)
        _tts_cache.clear()
        logger.info("TTS cache cleared: %d entries removed", count)
        return count
