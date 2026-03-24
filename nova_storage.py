"""
Nova File Storage Module

Handles file uploads, storage, and lifecycle management.
Supports local filesystem and optional S3 (future).
"""

from __future__ import annotations

import logging
import mimetypes
import os
import shutil
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Configuration
UPLOAD_DIR = Path(__file__).resolve().parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
ALLOWED_TYPES = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/msword": "doc",
    "text/plain": "txt",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/vnd.ms-excel": "xls",
    "text/csv": "csv",
}


# ---------------------------------------------------------------------------
# File Validation
# ---------------------------------------------------------------------------


def validate_file(file_path: str, file_size: int) -> Tuple[bool, str]:
    """Validate uploaded file.

    Args:
        file_path: Path to file
        file_size: File size in bytes

    Returns:
        (is_valid, error_message)
    """
    if not file_path or not os.path.exists(file_path):
        return False, "File does not exist"

    if file_size > MAX_FILE_SIZE:
        return False, f"File exceeds max size of {MAX_FILE_SIZE / 1024 / 1024:.0f} MB"

    # Check MIME type
    mime_type, _ = mimetypes.guess_type(file_path)
    if mime_type not in ALLOWED_TYPES:
        return False, f"File type not supported: {mime_type}"

    return True, ""


def get_file_type_from_path(file_path: str) -> Optional[str]:
    """Get file type from path.

    Args:
        file_path: Path to file

    Returns:
        File type ('pdf', 'docx', 'txt', etc.), or None
    """
    mime_type, _ = mimetypes.guess_type(file_path)
    return ALLOWED_TYPES.get(mime_type)


def get_file_extension(file_path: str) -> str:
    """Get file extension.

    Args:
        file_path: Path to file

    Returns:
        Extension (without dot), e.g., 'pdf'
    """
    return Path(file_path).suffix.lower().lstrip(".")


# ---------------------------------------------------------------------------
# File Storage Operations
# ---------------------------------------------------------------------------


def store_uploaded_file(
    source_path: str,
    conversation_id: str,
    filename: str,
) -> Optional[Dict[str, Any]]:
    """Store an uploaded file.

    Args:
        source_path: Temporary file path (from HTTP upload)
        conversation_id: UUID of conversation
        filename: Original filename

    Returns:
        File info dict with path and type, or None on error
    """
    try:
        # Validate
        file_size = os.path.getsize(source_path)
        is_valid, error = validate_file(source_path, file_size)
        if not is_valid:
            logger.error("File validation failed: %s", error)
            return None

        # Determine file type
        file_type = get_file_type_from_path(source_path)
        if not file_type:
            logger.error("Could not determine file type: %s", filename)
            return None

        # Create conversation upload directory
        conv_upload_dir = UPLOAD_DIR / conversation_id
        conv_upload_dir.mkdir(parents=True, exist_ok=True)

        # Generate unique filename to avoid collisions
        extension = get_file_extension(filename)
        base_name = Path(filename).stem
        import uuid

        unique_name = f"{base_name}_{uuid.uuid4().hex[:8]}.{extension}"
        dest_path = conv_upload_dir / unique_name

        # Copy file
        shutil.copy2(source_path, dest_path)
        logger.info("Stored file: %s -> %s", filename, dest_path)

        return {
            "filename": filename,
            "file_path": str(dest_path),
            "file_type": file_type,
            "size_bytes": file_size,
            "unique_name": unique_name,
        }

    except Exception as e:
        logger.error("Error storing file: %s", e, exc_info=True)
        return None


def delete_file(file_path: str) -> bool:
    """Delete a stored file.

    Args:
        file_path: Path to file

    Returns:
        True if successful
    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info("Deleted file: %s", file_path)
            return True
        return False
    except Exception as e:
        logger.error("Error deleting file: %s", e, exc_info=True)
        return False


def cleanup_conversation_files(conversation_id: str) -> int:
    """Delete all files for a conversation.

    Args:
        conversation_id: UUID of conversation

    Returns:
        Number of files deleted
    """
    try:
        conv_dir = UPLOAD_DIR / conversation_id
        if conv_dir.exists():
            count = len(list(conv_dir.glob("*")))
            shutil.rmtree(conv_dir)
            logger.info(
                "Cleaned up conversation files: %s (%d files)", conversation_id, count
            )
            return count
        return 0
    except Exception as e:
        logger.error("Error cleaning up files: %s", e, exc_info=True)
        return 0


# ---------------------------------------------------------------------------
# File Listing
# ---------------------------------------------------------------------------


def list_conversation_files(conversation_id: str) -> list[Dict[str, Any]]:
    """List all files in a conversation directory.

    Args:
        conversation_id: UUID of conversation

    Returns:
        List of file info dicts
    """
    try:
        conv_dir = UPLOAD_DIR / conversation_id
        if not conv_dir.exists():
            return []

        files = []
        for file_path in conv_dir.glob("*"):
            if file_path.is_file():
                files.append(
                    {
                        "filename": file_path.name,
                        "file_path": str(file_path),
                        "file_type": get_file_type_from_path(str(file_path)),
                        "size_bytes": file_path.stat().st_size,
                        "created_at": file_path.stat().st_mtime,
                    }
                )
        return files
    except Exception as e:
        logger.error("Error listing files: %s", e, exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Disk Space Management
# ---------------------------------------------------------------------------


def get_upload_dir_size() -> int:
    """Get total size of upload directory.

    Returns:
        Total size in bytes
    """
    try:
        total = 0
        for file_path in UPLOAD_DIR.rglob("*"):
            if file_path.is_file():
                total += file_path.stat().st_size
        return total
    except Exception as e:
        logger.error("Error calculating directory size: %s", e, exc_info=True)
        return 0


def cleanup_old_files(days_old: int = 30) -> int:
    """Clean up files older than specified days (optional).

    Args:
        days_old: Delete files older than this many days

    Returns:
        Number of files deleted
    """
    import time

    try:
        cutoff_time = time.time() - (days_old * 86400)
        deleted_count = 0

        for file_path in UPLOAD_DIR.rglob("*"):
            if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                try:
                    file_path.unlink()
                    deleted_count += 1
                except Exception as e:
                    logger.warning("Could not delete old file %s: %s", file_path, e)

        if deleted_count > 0:
            logger.info("Cleaned up %d old files", deleted_count)

        return deleted_count

    except Exception as e:
        logger.error("Error cleaning up old files: %s", e, exc_info=True)
        return 0


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


def health_check() -> bool:
    """Check if upload directory is accessible.

    Returns:
        True if healthy
    """
    try:
        # Try to write and delete a test file
        test_file = UPLOAD_DIR / ".health_check"
        test_file.write_text("ok")
        test_file.unlink()
        return True
    except Exception as e:
        logger.error("Storage health check failed: %s", e, exc_info=True)
        return False
