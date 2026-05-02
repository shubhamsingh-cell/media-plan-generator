"""Tests for ``apis.data`` -- Stack Overflow Survey + Lightcast Skills.

These tests are stdlib-only, fast, and never touch the live network. The
SO Survey download path is exercised via a monkeypatched
``urllib.request.urlopen`` that returns a tiny in-memory ZIP, so we can
verify the index-build path without pulling 17 MB.

Test groups:
    1. Import-success  -- modules import cleanly under Python 3.10+.
    2. Graceful degradation -- both ``lookup_*`` functions return error
       envelopes (not exceptions) when the underlying data is missing.
    3. SO Survey index logic -- mocked download produces correct salary,
       language, country, admiration, and experience metrics.
    4. Lightcast index logic -- CSV + JSON loaders, fuzzy fallback, cat
       filter, exact > substring > fuzzy ranking.
"""

from __future__ import annotations

import csv
import io
import json
import sys
import zipfile
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ─── 1. Import-success tests ──────────────────────────────────────────────────


def test_apis_data_package_imports() -> None:
    """``apis.data`` package itself imports without side effects."""
    import apis.data  # noqa: F401


def test_stack_overflow_module_imports() -> None:
    """SO Survey module exposes its public entrypoint."""
    from apis.data.stack_overflow_survey import (
        DATASET_URL,
        LICENSE,
        SOURCE_NAME,
        lookup_so_survey,
    )

    assert callable(lookup_so_survey)
    assert SOURCE_NAME == "Stack Overflow 2025 Developer Survey"
    assert LICENSE == "Open Database License"
    assert DATASET_URL.startswith("https://survey.stackoverflow.co/")


def test_lightcast_module_imports() -> None:
    """Lightcast module exposes both lookup entrypoints."""
    from apis.data.lightcast_skills import (
        LICENSE,
        SOURCE_NAME,
        lookup_lightcast_occupation,
        lookup_lightcast_skill,
    )

    assert callable(lookup_lightcast_skill)
    assert callable(lookup_lightcast_occupation)
    assert SOURCE_NAME.startswith("Lightcast Open Skills")
    assert "CC BY 4.0" in LICENSE


# ─── 2. Graceful-degradation tests ────────────────────────────────────────────


def test_so_survey_returns_error_when_download_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When the dataset cannot be reached, callers get an error dict."""
    from apis.data import stack_overflow_survey as so

    so.reset_cache_for_tests()
    # Redirect the cache file paths into a writable temp dir.
    monkeypatch.setattr(so, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(so, "CSV_PATH", tmp_path / "stack_overflow_survey_2025.csv")
    monkeypatch.setattr(so, "ZIP_PATH", tmp_path / "stack_overflow_survey_2025.zip")

    # Force the urlopen call to blow up so we hit the failure path.
    def boom(*args: Any, **kwargs: Any) -> Any:
        import urllib.error

        raise urllib.error.URLError("simulated network failure")

    monkeypatch.setattr("urllib.request.urlopen", boom)

    result = so.lookup_so_survey("salary", timeout=1)
    assert "error" in result
    assert result["source"] == "Stack Overflow 2025 Developer Survey"
    assert "dataset unavailable" in result["error"].lower()


def test_so_survey_rejects_unknown_metric() -> None:
    """Unknown metric names short-circuit before touching the dataset."""
    from apis.data import stack_overflow_survey as so

    result = so.lookup_so_survey("totally_made_up_metric")
    assert "error" in result
    assert "unknown metric" in result["error"].lower()
    assert result["source"] == "Stack Overflow 2025 Developer Survey"


def test_so_survey_rejects_empty_metric() -> None:
    """Empty metric returns the missing-arg error envelope."""
    from apis.data import stack_overflow_survey as so

    result = so.lookup_so_survey("")
    assert "error" in result
    assert "metric is required" in result["error"].lower()


def test_lightcast_skill_returns_error_when_data_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No file present -> graceful error pointing at the download URL."""
    from apis.data import lightcast_skills as lc

    lc.reset_cache_for_tests()
    monkeypatch.setattr(lc, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(
        lc,
        "_SKILL_PATHS",
        (
            tmp_path / "lightcast_open_skills.json",
            tmp_path / "lightcast_open_skills.csv",
        ),
    )

    result = lc.lookup_lightcast_skill("python")
    assert "error" in result
    assert "lightcast.io/open-skills" in result["error"]
    assert result["source"] == "Lightcast Open Skills (2026 release)"


def test_lightcast_occupation_returns_error_when_data_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Same graceful degradation for the occupations entrypoint."""
    from apis.data import lightcast_skills as lc

    lc.reset_cache_for_tests()
    monkeypatch.setattr(lc, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(
        lc,
        "_TITLE_PATHS",
        (
            tmp_path / "lightcast_open_titles.json",
            tmp_path / "lightcast_open_titles.csv",
        ),
    )

    result = lc.lookup_lightcast_occupation("software engineer")
    assert "error" in result
    assert "Lightcast Open Titles" in result["source"]


def test_lightcast_skill_rejects_empty_query() -> None:
    """Empty query returns error envelope without touching disk."""
    from apis.data import lightcast_skills as lc

    result = lc.lookup_lightcast_skill("")
    assert "error" in result
    assert "non-empty" in result["error"]


def test_lightcast_occupation_rejects_empty_query() -> None:
    """Empty occupation title returns error envelope."""
    from apis.data import lightcast_skills as lc

    result = lc.lookup_lightcast_occupation("   ")
    assert "error" in result
    assert "non-empty" in result["error"]


# ─── 3. SO Survey -- mocked download + index logic ────────────────────────────


def _build_fake_so_csv() -> bytes:
    """Build a tiny SO-Survey-shaped CSV that exercises every metric."""
    rows = [
        {
            "DevType": "Full-stack developer",
            "Country": "United States",
            "LanguageHaveWorkedWith": "Python;JavaScript",
            "LanguageAdmired": "Rust;Python",
            "ToolsTechAdmired": "Docker;Git",
            "ConvertedCompYearly": "100000",
            "YearsCode": "5",
        },
        {
            "DevType": "Full-stack developer;Backend developer",
            "Country": "Germany",
            "LanguageHaveWorkedWith": "Python;Go",
            "LanguageAdmired": "Rust",
            "ToolsTechAdmired": "Kubernetes;Docker",
            "ConvertedCompYearly": "80000",
            "YearsCode": "Less than 1 year",
        },
        {
            "DevType": "Data scientist",
            "Country": "United States",
            "LanguageHaveWorkedWith": "Python;R",
            "LanguageAdmired": "",
            "ToolsTechAdmired": "PyTorch",
            "ConvertedCompYearly": "150000",
            "YearsCode": "More than 50 years",
        },
        # Row with missing comp -- excluded from salary metric.
        {
            "DevType": "Hobbyist",
            "Country": "Brazil",
            "LanguageHaveWorkedWith": "JavaScript",
            "LanguageAdmired": "TypeScript",
            "ToolsTechAdmired": "VS Code",
            "ConvertedCompYearly": "",
            "YearsCode": "10",
        },
    ]
    fieldnames = [
        "DevType",
        "Country",
        "LanguageHaveWorkedWith",
        "LanguageAdmired",
        "ToolsTechAdmired",
        "ConvertedCompYearly",
        "YearsCode",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def _build_fake_so_zip() -> bytes:
    """Wrap the fake CSV in a ZIP that mirrors the real SO bundle layout."""
    csv_bytes = _build_fake_so_csv()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("survey-results-public.csv", csv_bytes)
        zf.writestr("README.txt", "fake readme")
    return buf.getvalue()


class _FakeResponse:
    """Minimal urlopen-result mock supporting ``.read(size)`` + context-mgr."""

    def __init__(self, payload: bytes) -> None:
        self._stream = io.BytesIO(payload)

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size if size > 0 else -1)

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *args: Any) -> None:
        self._stream.close()


@pytest.fixture
def primed_so_survey(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Yield the SO module with its dataset prebuilt from a tiny mock ZIP."""
    from apis.data import stack_overflow_survey as so

    so.reset_cache_for_tests()
    monkeypatch.setattr(so, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(so, "CSV_PATH", tmp_path / "so2025.csv")
    monkeypatch.setattr(so, "ZIP_PATH", tmp_path / "so2025.zip")

    fake_zip_bytes = _build_fake_so_zip()

    def fake_urlopen(*args: Any, **kwargs: Any) -> _FakeResponse:
        return _FakeResponse(fake_zip_bytes)

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    yield so
    so.reset_cache_for_tests()


def test_so_survey_salary_metric(primed_so_survey: Any) -> None:
    """Salary metric returns median + IQR over the mocked rows."""
    out = primed_so_survey.lookup_so_survey("salary", timeout=1)
    assert "error" not in out, out
    assert out["metric"] == "salary"
    assert out["source"] == "Stack Overflow 2025 Developer Survey"
    assert out["license"] == "Open Database License"

    # 4 comp values total; one row had blank comp -> 3 records contribute.
    # The Full-stack;Backend row produces TWO records (one per role) -> 4 total.
    # Comps in sorted order: 80000, 80000, 100000, 150000 -> median = 90000.
    assert out["n_respondents"] >= 3
    assert out["result"]["median_usd"] is not None
    assert out["result"]["p25_usd"] <= out["result"]["median_usd"]
    assert out["result"]["p75_usd"] >= out["result"]["median_usd"]


def test_so_survey_salary_metric_with_filter(primed_so_survey: Any) -> None:
    """Role + country filters narrow the matched salary records."""
    out = primed_so_survey.lookup_so_survey(
        "salary", role="Full-stack", country="United States", timeout=1
    )
    assert "error" not in out, out
    assert out["filters_applied"] == {
        "role": "Full-stack",
        "country": "United States",
    }
    assert out["n_respondents"] == 1
    assert out["result"]["median_usd"] == 100000.0


def test_so_survey_language_use(primed_so_survey: Any) -> None:
    """language_use without a filter returns top-N as a list."""
    out = primed_so_survey.lookup_so_survey("language_use", timeout=1)
    assert "error" not in out, out
    assert isinstance(out["result"], list)
    names = [item["name"] for item in out["result"]]
    assert "Python" in names
    assert "JavaScript" in names


def test_so_survey_language_use_filter(primed_so_survey: Any) -> None:
    """language_use with a filter returns just that language's stats."""
    out = primed_so_survey.lookup_so_survey(
        "language_use", language="Python", timeout=1
    )
    assert "error" not in out, out
    assert out["result"]["name"] == "Python"
    assert out["result"]["count"] >= 3


def test_so_survey_country_distribution(primed_so_survey: Any) -> None:
    """Country distribution returns counts + percentage shares."""
    out = primed_so_survey.lookup_so_survey("country_distribution", timeout=1)
    assert "error" not in out, out
    countries = {item["name"]: item["count"] for item in out["result"]}
    assert countries.get("United States") == 2
    assert countries.get("Germany") == 1


def test_so_survey_tech_admiration(primed_so_survey: Any) -> None:
    """tech_admiration returns top tools + top admired languages."""
    out = primed_so_survey.lookup_so_survey("tech_admiration", timeout=1)
    assert "error" not in out, out
    tools = {item["name"] for item in out["result"]["top_admired_tools"]}
    langs = {item["name"] for item in out["result"]["top_admired_languages"]}
    assert "Docker" in tools  # 2 mentions
    assert "Rust" in langs  # 2 mentions


def test_so_survey_experience(primed_so_survey: Any) -> None:
    """Experience metric translates SO ranges to floats."""
    out = primed_so_survey.lookup_so_survey("experience", timeout=1)
    assert "error" not in out, out
    # YearsCode values: 5, 0.5 (Less than 1), 51 (More than 50), 10
    assert out["n_respondents"] == 4
    assert out["result"]["mean"] is not None
    assert out["result"]["median"] is not None


def test_so_survey_dataset_cached_after_first_call(primed_so_survey: Any) -> None:
    """The CSV file exists and the in-memory dataset is reused on call #2."""
    primed_so_survey.lookup_so_survey("salary", timeout=1)
    assert primed_so_survey.CSV_PATH.exists()
    # Trigger the cached path by calling a second metric -- no download retry.
    out2 = primed_so_survey.lookup_so_survey("country_distribution", timeout=1)
    assert "error" not in out2


# ─── 4. Lightcast index-logic tests (no download required) ────────────────────


def _write_lightcast_csv(path: Path) -> None:
    """Write a tiny Lightcast skills CSV for the index logic tests."""
    rows = [
        ("KS001", "Python", "Information Technology", "Hard Skill"),
        ("KS002", "PyTorch", "Information Technology", "Hard Skill"),
        ("KS003", "Communication", "Soft Skills", "Soft Skill"),
        ("KS004", "Java", "Information Technology", "Hard Skill"),
        ("KS005", "Project Management", "Business", "Soft Skill"),
    ]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["id", "name", "category", "type"])
        writer.writerows(rows)


def _write_lightcast_titles_json(path: Path) -> None:
    """Write a tiny Open Titles JSON exercising the dict.data unwrapping."""
    payload = {
        "data": [
            {
                "id": "ET001",
                "name": "Software Engineer",
                "category": "IT",
                "type": "Title",
            },
            {
                "id": "ET002",
                "name": "Software Developer",
                "category": "IT",
                "type": "Title",
            },
            {
                "id": "ET003",
                "name": "Data Scientist",
                "category": "Data",
                "type": "Title",
            },
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


@pytest.fixture
def primed_lightcast(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Set up the Lightcast module pointed at fixture data files."""
    from apis.data import lightcast_skills as lc

    lc.reset_cache_for_tests()
    skills_csv = tmp_path / "lightcast_open_skills.csv"
    titles_json = tmp_path / "lightcast_open_titles.json"
    _write_lightcast_csv(skills_csv)
    _write_lightcast_titles_json(titles_json)

    monkeypatch.setattr(lc, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(lc, "_SKILL_PATHS", (skills_csv,))
    monkeypatch.setattr(lc, "_TITLE_PATHS", (titles_json,))
    yield lc
    lc.reset_cache_for_tests()


def test_lightcast_skill_exact_match(primed_lightcast: Any) -> None:
    """Exact name match takes priority over substring/fuzzy hits."""
    out = primed_lightcast.lookup_lightcast_skill("Python")
    assert "error" not in out
    assert out["query"] == "Python"
    assert out["matches"][0]["name"] == "Python"
    assert out["matches"][0]["id"] == "KS001"


def test_lightcast_skill_substring_match(primed_lightcast: Any) -> None:
    """Partial query matches the corresponding Lightcast entry."""
    out = primed_lightcast.lookup_lightcast_skill("manage", limit=5)
    assert "error" not in out
    names = [m["name"] for m in out["matches"]]
    assert "Project Management" in names


def test_lightcast_skill_fuzzy_fallback(primed_lightcast: Any) -> None:
    """Misspelt query falls back to difflib close-match."""
    # "Pythn" -> close match to "Python" via difflib.
    out = primed_lightcast.lookup_lightcast_skill("Pythn", fuzzy=True, limit=3)
    assert "error" not in out
    names = [m["name"] for m in out["matches"]]
    assert "Python" in names


def test_lightcast_skill_fuzzy_disabled(primed_lightcast: Any) -> None:
    """With fuzzy=False, a strict misspelling returns zero matches."""
    out = primed_lightcast.lookup_lightcast_skill("Zzzznotaskill", fuzzy=False, limit=5)
    assert "error" not in out
    assert out["matches"] == []


def test_lightcast_skill_category_filter(primed_lightcast: Any) -> None:
    """Category filter excludes records outside that category."""
    out = primed_lightcast.lookup_lightcast_skill(
        "py", fuzzy=True, limit=5, category="Information Technology"
    )
    assert "error" not in out
    for match in out["matches"]:
        assert match["category"] == "Information Technology"


def test_lightcast_skill_limit_clamps(primed_lightcast: Any) -> None:
    """``limit`` is clamped to a sensible range and never exceeds dataset size."""
    out = primed_lightcast.lookup_lightcast_skill("a", limit=999)
    assert "error" not in out
    assert len(out["matches"]) <= 50  # internal cap
    assert len(out["matches"]) <= 5  # dataset only has 5 rows


def test_lightcast_occupation_match(primed_lightcast: Any) -> None:
    """Occupation lookup walks the Open Titles JSON correctly."""
    out = primed_lightcast.lookup_lightcast_occupation("Software Engineer")
    assert "error" not in out
    assert out["matches"][0]["name"] == "Software Engineer"
    assert out["source"] == "Lightcast Open Titles (2026 release)"


def test_lightcast_occupation_substring(primed_lightcast: Any) -> None:
    """Substring search matches both 'Software Engineer' and 'Software Developer'."""
    out = primed_lightcast.lookup_lightcast_occupation("software", limit=5)
    assert "error" not in out
    names = {m["name"] for m in out["matches"]}
    assert {"Software Engineer", "Software Developer"}.issubset(names)


def test_lightcast_skill_index_cached_across_calls(primed_lightcast: Any) -> None:
    """The skills file is parsed once; subsequent calls hit the in-mem index."""
    primed_lightcast.lookup_lightcast_skill("Python")
    # Replace the file content; if caching works, results stay the same.
    skill_path = primed_lightcast._SKILL_PATHS[0]
    skill_path.write_text("id,name,category,type\nXX,Other,X,Hard\n", encoding="utf-8")
    out2 = primed_lightcast.lookup_lightcast_skill("Python")
    assert out2["matches"][0]["name"] == "Python"  # served from cache
