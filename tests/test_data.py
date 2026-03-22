"""Data file validation for knowledge base and configuration."""

import json
from pathlib import Path

import pytest

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Maximum file size: 5 MB
MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024


class TestDataDirectory:
    """The data/ directory must exist and contain knowledge base files."""

    def test_data_directory_exists(self) -> None:
        """data/ directory must exist."""
        assert DATA_DIR.exists(), "data/ directory does not exist"
        assert DATA_DIR.is_dir(), "data/ is not a directory"

    def test_has_json_files(self) -> None:
        """data/ must contain at least one JSON file."""
        json_files = list(DATA_DIR.glob("*.json"))
        assert len(json_files) > 0, "No JSON files found in data/"


class TestJsonValidity:
    """All JSON files in data/ must be parseable."""

    @pytest.fixture(scope="class")
    def json_files(self) -> list[Path]:
        """Collect all JSON files in data/."""
        return sorted(DATA_DIR.glob("*.json"))

    def test_all_json_files_parse(self, json_files: list[Path]) -> None:
        """Each JSON file must be valid, parseable JSON."""
        invalid: list[str] = []
        for jf in json_files:
            try:
                json.loads(jf.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                invalid.append(f"{jf.name}: {exc}")

        assert not invalid, f"Invalid JSON files: {'; '.join(invalid)}"

    def test_json_files_under_size_limit(self, json_files: list[Path]) -> None:
        """Each JSON file must be under 5 MB (sanity check)."""
        oversized: list[str] = []
        for jf in json_files:
            size = jf.stat().st_size
            if size > MAX_FILE_SIZE_BYTES:
                mb = size / (1024 * 1024)
                oversized.append(f"{jf.name}: {mb:.1f} MB")

        assert not oversized, f"JSON files over 5 MB limit: {'; '.join(oversized)}"


class TestKnowledgeBaseFiles:
    """Key knowledge base files must exist."""

    EXPECTED_KB_FILES = [
        "channels_db.json",
        "recruitment_industry_knowledge.json",
    ]

    @pytest.mark.parametrize("filename", EXPECTED_KB_FILES)
    def test_kb_file_exists(self, filename: str) -> None:
        """Each expected knowledge base file should exist."""
        path = DATA_DIR / filename
        assert path.exists(), f"Missing knowledge base file: {filename}"
