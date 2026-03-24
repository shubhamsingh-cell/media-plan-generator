"""
Nova Voice Module

Handles voice input/output for Nova chatbot.
- Voice-to-text using OpenAI Whisper or browser Web Speech API
- Text-to-speech using external TTS service
"""

from __future__ import annotations

import io
import logging
import os
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Try to import voice libraries
try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


# ---------------------------------------------------------------------------
# Text-to-Speech (TTS)
# ---------------------------------------------------------------------------


def text_to_speech(
    text: str,
    voice: str = "nova",
    speed: float = 1.0,
) -> Optional[bytes]:
    """Convert text to speech audio.

    Uses OpenAI TTS API. For frontend, use browser Web Speech API instead.

    Args:
        text: Text to convert
        voice: Voice name ('nova', 'alloy', 'echo', 'fable', 'onyx', 'shimmer')
        speed: Speech speed (0.25-4.0)

    Returns:
        Audio bytes (MP3), or None on error
    """
    if not REQUESTS_AVAILABLE:
        logger.warning("requests library not available for TTS")
        return None

    api_key = os.getenv("OPENAI_API_KEY") or ""
    if not api_key:
        logger.warning("OPENAI_API_KEY not set; cannot generate TTS")
        return None

    try:
        # Truncate text if too long (API limit ~4000 chars)
        if len(text) > 4000:
            text = text[:3997] + "..."
            logger.warning("Text truncated for TTS")

        response = requests.post(
            "https://api.openai.com/v1/audio/speech",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "tts-1",
                "input": text,
                "voice": voice,
                "speed": speed,
            },
            timeout=30,
        )

        if response.status_code == 200:
            logger.info("Generated TTS audio (%d bytes)", len(response.content))
            return response.content

        logger.error("TTS API error: %s", response.text)
        return None

    except Exception as e:
        logger.error("Error generating TTS: %s", e, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Voice-to-Text (Speech Recognition)
# ---------------------------------------------------------------------------


def transcribe_audio(audio_bytes: bytes, language: str = "en") -> Optional[str]:
    """Transcribe audio to text using OpenAI Whisper.

    Args:
        audio_bytes: Audio data (WAV, MP3, or other format)
        language: Language code (e.g., 'en', 'es', 'fr')

    Returns:
        Transcribed text, or None on error
    """
    if not REQUESTS_AVAILABLE:
        logger.warning("requests library not available for transcription")
        return None

    api_key = os.getenv("OPENAI_API_KEY") or ""
    if not api_key:
        logger.warning("OPENAI_API_KEY not set; cannot transcribe audio")
        return None

    try:
        files = {
            "file": ("audio.wav", io.BytesIO(audio_bytes), "audio/wav"),
        }
        data = {
            "model": "whisper-1",
            "language": language,
        }

        response = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {api_key}"},
            files=files,
            data=data,
            timeout=60,
        )

        if response.status_code == 200:
            result = response.json()
            text = result.get("text", "").strip()
            logger.info("Transcribed audio: %s", text[:100])
            return text if text else None

        logger.error("Whisper API error: %s", response.text)
        return None

    except Exception as e:
        logger.error("Error transcribing audio: %s", e, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Voice Configuration
# ---------------------------------------------------------------------------


class VoiceConfig:
    """Voice settings for a conversation."""

    def __init__(
        self,
        enabled: bool = False,
        voice: str = "nova",
        tts_enabled: bool = False,
        stt_enabled: bool = True,
        language: str = "en",
        speed: float = 1.0,
    ):
        """Initialize voice config.

        Args:
            enabled: Whether voice features are enabled
            voice: TTS voice name
            tts_enabled: Enable text-to-speech
            stt_enabled: Enable speech-to-text
            language: Language code
            speed: TTS speech speed
        """
        self.enabled = enabled
        self.voice = voice
        self.tts_enabled = tts_enabled
        self.stt_enabled = stt_enabled
        self.language = language
        self.speed = max(0.25, min(4.0, speed))  # Clamp to valid range

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for serialization."""
        return {
            "enabled": self.enabled,
            "voice": self.voice,
            "tts_enabled": self.tts_enabled,
            "stt_enabled": self.stt_enabled,
            "language": self.language,
            "speed": self.speed,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> VoiceConfig:
        """Create from dict."""
        return cls(
            enabled=data.get("enabled", False),
            voice=data.get("voice", "nova"),
            tts_enabled=data.get("tts_enabled", False),
            stt_enabled=data.get("stt_enabled", True),
            language=data.get("language", "en"),
            speed=data.get("speed", 1.0),
        )


# Supported voices
AVAILABLE_VOICES = [
    {"name": "nova", "description": "Warm, energetic female voice"},
    {"name": "alloy", "description": "Bright, upbeat male voice"},
    {"name": "echo", "description": "Deep, resonant male voice"},
    {"name": "fable", "description": "Classic, warm male voice"},
    {"name": "onyx", "description": "Deep, professional female voice"},
    {"name": "shimmer", "description": "Bright, enthusiastic female voice"},
]

SUPPORTED_LANGUAGES = [
    {"code": "en", "name": "English"},
    {"code": "es", "name": "Spanish"},
    {"code": "fr", "name": "French"},
    {"code": "de", "name": "German"},
    {"code": "it", "name": "Italian"},
    {"code": "pt", "name": "Portuguese"},
    {"code": "zh", "name": "Chinese"},
    {"code": "ja", "name": "Japanese"},
    {"code": "ko", "name": "Korean"},
]


def get_available_voices() -> list[Dict[str, str]]:
    """Get list of available TTS voices."""
    return AVAILABLE_VOICES


def get_supported_languages() -> list[Dict[str, str]]:
    """Get list of supported languages."""
    return SUPPORTED_LANGUAGES


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def validate_voice_config(config: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate voice configuration.

    Args:
        config: Configuration dict

    Returns:
        (is_valid, error_message)
    """
    if not isinstance(config, dict):
        return False, "Config must be a dict"

    voice = config.get("voice")
    if voice and not any(v["name"] == voice for v in AVAILABLE_VOICES):
        return False, f"Invalid voice: {voice}"

    language = config.get("language")
    if language and not any(l["code"] == language for l in SUPPORTED_LANGUAGES):
        return False, f"Invalid language: {language}"

    speed = config.get("speed")
    if speed:
        try:
            speed_val = float(speed)
            if speed_val < 0.25 or speed_val > 4.0:
                return False, "Speed must be between 0.25 and 4.0"
        except (TypeError, ValueError):
            return False, "Speed must be a number"

    return True, ""


def health_check() -> bool:
    """Check if voice service is available.

    Returns:
        True if OpenAI API is accessible
    """
    api_key = os.getenv("OPENAI_API_KEY")
    return bool(api_key) and REQUESTS_AVAILABLE
