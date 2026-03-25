"""Text-to-Speech POST route handler using ElevenLabs API.

Extracted as a route module following the existing pattern.  Handles:
- POST /api/tts

Dependencies: stdlib only (urllib, json).
"""

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

# ElevenLabs configuration
_ELEVENLABS_API_KEY: str = os.environ.get("ELEVENLABS_API_KEY") or ""
_ELEVENLABS_VOICE_ID: str = "21m00Tcm4TlvDq8ikWAM"  # Rachel voice
_ELEVENLABS_MODEL: str = "eleven_monolingual_v1"
_MAX_TEXT_LENGTH: int = 5000


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_tts_post_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch TTS POST routes.  Returns True if handled."""
    if path == "/api/tts":
        _handle_tts(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# Individual route handlers
# ---------------------------------------------------------------------------


def _handle_tts(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/tts -- convert text to speech using ElevenLabs API.

    Request body: {"text": "...", "voice": "nova"}
    Response: audio/mpeg binary stream.
    """
    try:
        body = handler._read_json_body()
    except (ValueError, json.JSONDecodeError) as exc:
        logger.warning("TTS: invalid request body: %s", exc)
        handler._send_json({"error": "Invalid JSON body"}, 400)
        return

    text: str = (body.get("text") or "").strip()
    if not text:
        handler._send_json({"error": "No text provided"}, 400)
        return

    api_key = _ELEVENLABS_API_KEY
    if not api_key:
        handler._send_json({"error": "ElevenLabs not configured"}, 503)
        return

    # Truncate to max length
    text = text[:_MAX_TEXT_LENGTH]

    voice_id = _ELEVENLABS_VOICE_ID
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"

    payload = json.dumps(
        {
            "text": text,
            "model_id": _ELEVENLABS_MODEL,
            "voice_settings": {
                "stability": 0.5,
                "similarity_boost": 0.75,
            },
        }
    ).encode("utf-8")

    try:
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "xi-api-key": api_key,
                "Accept": "audio/mpeg",
            },
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            audio_data: bytes = resp.read()

        handler.send_response(200)
        handler.send_header("Content-Type", "audio/mpeg")
        handler.send_header("Content-Length", str(len(audio_data)))
        cors_origin = handler._get_cors_origin()
        if cors_origin:
            handler.send_header("Access-Control-Allow-Origin", cors_origin)
        handler.end_headers()
        handler.wfile.write(audio_data)

    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8", errors="replace")[:300]
        except OSError:
            pass
        logger.error(
            "TTS ElevenLabs HTTP %d: %s", http_err.code, error_body, exc_info=True
        )
        handler._send_json({"error": "TTS generation failed"}, 500)

    except (urllib.error.URLError, OSError) as exc:
        logger.error("TTS request failed: %s", exc, exc_info=True)
        handler._send_json({"error": "TTS generation failed"}, 500)
