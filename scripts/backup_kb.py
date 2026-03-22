#!/usr/bin/env python3
"""Knowledge Base Backup & Restore System for Nova AI Suite.

Creates timestamped ZIP backups of the data/ directory (excluding api_cache/)
and provides restore functionality with validation.

Usage:
    python scripts/backup_kb.py backup [--data-dir DATA_DIR] [--backup-dir BACKUP_DIR]
    python scripts/backup_kb.py restore <backup_path> [--data-dir DATA_DIR]
"""

import argparse
import json
import logging
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Directories to exclude from backups
_EXCLUDE_DIRS = {"api_cache", "backups", "generated_docs", "__pycache__"}

# Maximum number of backups to retain
_MAX_BACKUPS = 7

# Expected JSON files that a valid backup should contain at least one of
_EXPECTED_JSON_FILES = {
    "channels_db.json",
    "recruitment_benchmarks_deep.json",
    "recruitment_industry_knowledge.json",
    "joveo_publishers.json",
}


def backup_knowledge_base(
    data_dir: str,
    backup_dir: str = "",
) -> str:
    """Create a timestamped ZIP backup of the knowledge base.

    Args:
        data_dir: Path to the data/ directory to back up.
        backup_dir: Path where backups are stored. Defaults to data_dir/backups/.

    Returns:
        Absolute path of the created backup file.

    Raises:
        FileNotFoundError: If data_dir does not exist.
        RuntimeError: If no files were added to the backup.
    """
    data_path = Path(data_dir).resolve()
    if not data_path.is_dir():
        raise FileNotFoundError(f"Data directory not found: {data_path}")

    backup_path = Path(backup_dir).resolve() if backup_dir else data_path / "backups"
    backup_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    zip_name = f"kb_backup_{timestamp}.zip"
    zip_path = backup_path / zip_name

    file_count = 0
    try:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for item in sorted(data_path.rglob("*")):
                # Skip excluded directories and their contents
                rel = item.relative_to(data_path)
                if any(part in _EXCLUDE_DIRS for part in rel.parts):
                    continue
                # Skip lock files and temporary files
                if item.suffix in (".lock", ".tmp"):
                    continue
                if item.is_file():
                    zf.write(item, arcname=str(rel))
                    file_count += 1

        if file_count == 0:
            zip_path.unlink(missing_ok=True)
            raise RuntimeError(f"No files found in {data_path} to back up")

        logger.info(
            "Backup created: %s (%d files, %.1f KB)",
            zip_path.name,
            file_count,
            zip_path.stat().st_size / 1024,
        )
    except Exception:
        # Clean up partial ZIP on failure
        zip_path.unlink(missing_ok=True)
        raise

    # Prune old backups, keeping only the most recent _MAX_BACKUPS
    _prune_old_backups(backup_path)

    return str(zip_path)


def _prune_old_backups(backup_path: Path) -> None:
    """Remove old backups, keeping only the last _MAX_BACKUPS files.

    Args:
        backup_path: Directory containing backup ZIP files.
    """
    backups = sorted(
        backup_path.glob("kb_backup_*.zip"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old_backup in backups[_MAX_BACKUPS:]:
        try:
            old_backup.unlink()
            logger.info("Pruned old backup: %s", old_backup.name)
        except OSError as e:
            logger.warning("Failed to prune backup %s: %s", old_backup.name, e)


def restore_knowledge_base(backup_path: str, data_dir: str) -> bool:
    """Restore a knowledge base from a backup ZIP.

    Validates the ZIP contains expected JSON files before extracting.
    Extracts directly into the data directory, overwriting existing files.

    Args:
        backup_path: Path to the backup ZIP file.
        data_dir: Path to the data/ directory to restore into.

    Returns:
        True on successful restore, False on validation or extraction failure.
    """
    zip_file = Path(backup_path).resolve()
    data_path = Path(data_dir).resolve()

    if not zip_file.is_file():
        logger.error("Backup file not found: %s", zip_file)
        return False

    if not data_path.is_dir():
        logger.error("Data directory not found: %s", data_path)
        return False

    # Validate the ZIP file
    try:
        with zipfile.ZipFile(zip_file, "r") as zf:
            # Check ZIP integrity
            bad_file = zf.testzip()
            if bad_file is not None:
                logger.error("Corrupt file in backup: %s", bad_file)
                return False

            names = set(zf.namelist())
            if not names:
                logger.error("Backup ZIP is empty")
                return False

            # Check for at least one expected JSON file
            found_expected = names & _EXPECTED_JSON_FILES
            if not found_expected:
                logger.error(
                    "Backup does not contain any expected KB files. "
                    "Expected at least one of: %s",
                    ", ".join(sorted(_EXPECTED_JSON_FILES)),
                )
                return False

            # Validate JSON files are parseable
            json_files = [n for n in names if n.endswith(".json")]
            for jf in json_files:
                try:
                    content = zf.read(jf)
                    json.loads(content)
                except (json.JSONDecodeError, ValueError) as e:
                    logger.error("Invalid JSON in backup file %s: %s", jf, e)
                    return False

            # Security: check for path traversal
            for name in names:
                member_path = (data_path / name).resolve()
                if not str(member_path).startswith(str(data_path)):
                    logger.error("Path traversal detected in backup: %s", name)
                    return False

            # All validations passed -- extract
            zf.extractall(data_path)
            logger.info(
                "Restored %d files from %s to %s",
                len(names),
                zip_file.name,
                data_path,
            )
            return True

    except zipfile.BadZipFile:
        logger.error("Not a valid ZIP file: %s", zip_file)
        return False
    except OSError as e:
        logger.error("OS error during restore: %s", e, exc_info=True)
        return False


def main() -> int:
    """CLI entry point for backup/restore operations."""
    parser = argparse.ArgumentParser(
        description="Knowledge Base Backup & Restore for Nova AI Suite"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # backup subcommand
    backup_parser = subparsers.add_parser("backup", help="Create a backup of data/")
    backup_parser.add_argument(
        "--data-dir",
        default="",
        help="Path to data/ directory (default: auto-detect from project root)",
    )
    backup_parser.add_argument(
        "--backup-dir",
        default="",
        help="Path to store backups (default: data/backups/)",
    )

    # restore subcommand
    restore_parser = subparsers.add_parser(
        "restore", help="Restore data/ from a backup"
    )
    restore_parser.add_argument("backup_path", help="Path to the backup ZIP file")
    restore_parser.add_argument(
        "--data-dir",
        default="",
        help="Path to data/ directory (default: auto-detect from project root)",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Auto-detect data directory from project root
    project_root = Path(__file__).resolve().parent.parent
    default_data_dir = str(project_root / "data")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if args.command == "backup":
        data_dir = args.data_dir or default_data_dir
        try:
            result = backup_knowledge_base(data_dir, args.backup_dir)
            print(f"Backup created: {result}")
            return 0
        except Exception as e:
            logger.error("Backup failed: %s", e, exc_info=True)
            print(f"ERROR: Backup failed -- {e}", file=sys.stderr)
            return 1

    elif args.command == "restore":
        data_dir = args.data_dir or default_data_dir
        success = restore_knowledge_base(args.backup_path, data_dir)
        if success:
            print("Restore completed successfully")
            return 0
        else:
            print("ERROR: Restore failed -- check logs above", file=sys.stderr)
            return 1

    return 1


if __name__ == "__main__":
    sys.exit(main())
