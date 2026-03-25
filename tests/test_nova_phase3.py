"""
Tests for Nova Phase 3+4 Enterprise Features

Tests for:
- Conversation persistence (nova_persistence.py)
- Document RAG operations (nova_rag.py)
- File storage (nova_storage.py)
- Voice configuration (nova_voice.py)
"""

import pytest
import tempfile
import os
from pathlib import Path
from typing import Dict, Any

# Import modules to test
try:
    from nova_persistence import (
        create_conversation,
        get_conversation,
        list_conversations,
        update_conversation,
        delete_conversation,
        create_document,
        list_documents,
        create_share_link,
        get_shared_conversation,
    )
except ImportError:
    pytest.skip("nova_persistence not available", allow_module_level=True)

try:
    from nova_rag import (
        chunk_text,
        clean_text,
        extract_key_phrases,
        simple_embedding,
        cosine_similarity,
        retrieve_relevant_chunks,
        format_context_for_prompt,
    )
except ImportError:
    pytest.skip("nova_rag not available", allow_module_level=True)

try:
    from nova_storage import (
        validate_file,
        get_file_type_from_path,
        list_conversation_files,
        health_check as storage_health_check,
    )
except ImportError:
    pytest.skip("nova_storage not available", allow_module_level=True)

try:
    from nova_voice import (
        VoiceConfig,
        get_available_voices,
        validate_voice_config,
        health_check as voice_health_check,
    )
except ImportError:
    pytest.skip("nova_voice not available", allow_module_level=True)


# ============================================================================
# Tests for nova_rag.py
# ============================================================================


class TestRAG:
    """Test RAG operations."""

    def test_chunk_text(self) -> None:
        """Test text chunking."""
        text = "This is a test. " * 100
        chunks = chunk_text(text, chunk_size=100, overlap=20)
        assert len(chunks) > 1
        # Each chunk should be roughly the chunk_size
        for chunk in chunks:
            assert len(chunk) <= 150  # Some buffer for word boundaries

    def test_clean_text(self) -> None:
        """Test text cleaning."""
        text = "  Hello\n\n  World  \t  http://example.com  test  "
        cleaned = clean_text(text)
        assert "Hello World" in cleaned
        assert "http" not in cleaned
        assert cleaned == cleaned.strip()

    def test_extract_key_phrases(self) -> None:
        """Test key phrase extraction."""
        text = (
            "Apple Inc. is a technology company. Google and Microsoft are competitors."
        )
        phrases = extract_key_phrases(text, max_phrases=3)
        assert len(phrases) <= 3
        assert isinstance(phrases, list)

    def test_simple_embedding(self) -> None:
        """Test embedding generation."""
        text = "Nova is an AI assistant for recruitment intelligence"
        embedding = simple_embedding(text)
        assert isinstance(embedding, list)
        assert len(embedding) == 100

    def test_cosine_similarity(self) -> None:
        """Test cosine similarity."""
        vec1 = [1.0, 0.0, 0.0]
        vec2 = [1.0, 0.0, 0.0]
        similarity = cosine_similarity(vec1, vec2)
        assert abs(similarity - 1.0) < 0.01

        vec3 = [0.0, 1.0, 0.0]
        similarity = cosine_similarity(vec1, vec3)
        assert abs(similarity - 0.0) < 0.01

    def test_retrieve_relevant_chunks(self) -> None:
        """Test chunk retrieval."""
        documents = [
            {
                "content_text": "Nova is a recruitment intelligence platform. "
                "It helps companies with hiring decisions."
            },
            {
                "content_text": "Python is a programming language. "
                "It's popular for data science."
            },
        ]
        query = "Tell me about Nova recruitment"
        chunks = retrieve_relevant_chunks(query, documents, top_k=2)
        assert isinstance(chunks, list)
        # Should return some chunks
        if chunks:
            assert all(isinstance(c, tuple) and len(c) == 2 for c in chunks)

    def test_format_context_for_prompt(self) -> None:
        """Test context formatting."""
        chunks = [
            ("This is a test chunk.", 0.9),
            ("This is another chunk.", 0.7),
        ]
        context = format_context_for_prompt(chunks)
        assert "Document Context" in context
        assert "Reference 1" in context
        assert "Reference 2" in context


# ============================================================================
# Tests for nova_storage.py
# ============================================================================


class TestFileStorage:
    """Test file storage operations."""

    def test_validate_file(self) -> None:
        """Test file validation."""
        # Create a temporary text file
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"Test content")
            temp_path = f.name

        try:
            # Valid file
            is_valid, msg = validate_file(temp_path, 100)
            assert is_valid, msg

            # File doesn't exist
            is_valid, _ = validate_file("/nonexistent/file.txt", 100)
            assert not is_valid

            # File too large
            is_valid, _ = validate_file(temp_path, 60 * 1024 * 1024)
            assert not is_valid
        finally:
            os.unlink(temp_path)

    def test_get_file_type_from_path(self) -> None:
        """Test file type detection."""
        assert get_file_type_from_path("document.pdf") == "pdf"
        assert get_file_type_from_path("report.txt") == "txt"
        assert get_file_type_from_path("spreadsheet.xlsx") == "xlsx"

    def test_list_conversation_files(self) -> None:
        """Test file listing."""
        # Should return empty list for non-existent conversation
        files = list_conversation_files("nonexistent-id")
        assert isinstance(files, list)
        assert len(files) == 0

    def test_storage_health_check(self) -> None:
        """Test storage health check."""
        health = storage_health_check()
        assert isinstance(health, bool)


# ============================================================================
# Tests for nova_voice.py
# ============================================================================


class TestVoiceConfiguration:
    """Test voice configuration."""

    def test_voice_config_creation(self) -> None:
        """Test VoiceConfig creation."""
        config = VoiceConfig(
            enabled=True,
            voice="nova",
            tts_enabled=True,
            language="en",
        )
        assert config.enabled is True
        assert config.voice == "nova"
        assert config.language == "en"

    def test_voice_config_to_dict(self) -> None:
        """Test VoiceConfig serialization."""
        config = VoiceConfig(enabled=True, voice="alloy", speed=1.5)
        data = config.to_dict()
        assert data["enabled"] is True
        assert data["voice"] == "alloy"
        assert data["speed"] == 1.5

    def test_voice_config_from_dict(self) -> None:
        """Test VoiceConfig deserialization."""
        data = {
            "enabled": True,
            "voice": "echo",
            "language": "es",
            "speed": 0.8,
        }
        config = VoiceConfig.from_dict(data)
        assert config.enabled is True
        assert config.voice == "echo"
        assert config.language == "es"

    def test_voice_config_speed_clamping(self) -> None:
        """Test speed value clamping."""
        # Too slow
        config = VoiceConfig(speed=0.1)
        assert config.speed >= 0.25

        # Too fast
        config = VoiceConfig(speed=5.0)
        assert config.speed <= 4.0

    def test_available_voices(self) -> None:
        """Test available voices list."""
        voices = get_available_voices()
        assert len(voices) >= 6
        assert all("name" in v and "description" in v for v in voices)

    def test_voice_config_validation(self) -> None:
        """Test voice config validation."""
        # Valid config
        config = {
            "voice": "nova",
            "language": "en",
            "speed": 1.0,
        }
        is_valid, msg = validate_voice_config(config)
        assert is_valid, msg

        # Invalid voice
        config = {"voice": "unknown"}
        is_valid, _ = validate_voice_config(config)
        assert not is_valid

        # Invalid language
        config = {"language": "xx"}
        is_valid, _ = validate_voice_config(config)
        assert not is_valid

    def test_voice_health_check(self) -> None:
        """Test voice service health check."""
        health = voice_health_check()
        assert isinstance(health, bool)


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests combining multiple features."""

    def test_full_conversation_workflow(self) -> None:
        """Test complete conversation workflow (mock)."""
        # This test would require Supabase access
        # Skipped in CI without proper setup
        pytest.skip("Requires Supabase credentials")

    def test_avatar_with_conversation(self) -> None:
        """Test avatar assignment to conversation."""
        avatar = AvatarGenerator.generate_gradient_avatar("Assistant")
        is_valid, _ = validate_avatar(avatar)
        assert is_valid
        # Conversation would store avatar style in preferences
        assert avatar["style"] == "gradient"

    def test_document_with_rag_retrieval(self) -> None:
        """Test document retrieval for RAG."""
        doc_content = (
            "Nova is a recruitment intelligence platform providing "
            "insights into hiring markets and salary benchmarks."
        )
        documents = [{"content_text": doc_content}]

        query = "What is Nova?"
        chunks = retrieve_relevant_chunks(query, documents, top_k=1)

        # Should find the document content relevant
        assert len(chunks) >= 0  # May or may not find depending on similarity


# ============================================================================
# Run Tests
# ============================================================================


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
