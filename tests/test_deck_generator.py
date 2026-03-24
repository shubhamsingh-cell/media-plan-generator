"""Tests for the 7-tier DeckGenerator fallback system.

Tests verify:
- Module imports cleanly
- DeckGenerator class instantiation
- Tier 7 (python-pptx) always works as offline fallback
- Usage tracking and monthly limits
- Graceful skipping when API keys are not set
- get_status() returns valid structure
- force_tier parameter routing
- _format_plan_as_text helper produces valid output
"""

import importlib
import os
import sys
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(autouse=True)
def _reset_usage() -> None:
    """Reset usage counters before each test."""
    import deck_generator

    with deck_generator._usage_lock:
        deck_generator._monthly_usage.clear()
        deck_generator._usage_month = ""


@pytest.fixture
def generator() -> "deck_generator.DeckGenerator":
    """Return a fresh DeckGenerator instance."""
    from deck_generator import DeckGenerator

    return DeckGenerator()


@pytest.fixture
def sample_data() -> dict:
    """Return minimal media plan data for testing."""
    return {
        "client_name": "TestCorp",
        "industry": "technology",
        "budget": "$50,000",
        "locations": ["San Francisco, CA"],
        "roles": ["Software Engineer", "DevOps Engineer"],
        "target_roles": [{"title": "Software Engineer"}, {"title": "DevOps Engineer"}],
        "campaign_goals": ["cost_efficiency", "speed_to_hire"],
        "duration": "3 months",
        "work_environment": "remote",
        "channels": [
            {
                "name": "LinkedIn",
                "budget": 20000,
                "estimated_reach": 50000,
                "cost_per_application": 15.50,
            },
            {
                "name": "Indeed",
                "budget": 15000,
                "estimated_reach": 80000,
                "cost_per_application": 8.25,
            },
        ],
        "recommendations": [
            "Increase LinkedIn budget by 15% for senior roles",
            "Add programmatic display for passive candidates",
        ],
        "benchmarks": {
            "avg_cpa": "$12.50",
            "avg_time_to_fill": "35 days",
        },
    }


class TestModuleImport:
    """Verify deck_generator module loads without errors."""

    def test_module_imports(self) -> None:
        """Module should import cleanly."""
        mod = importlib.import_module("deck_generator")
        assert mod is not None

    def test_class_exists(self) -> None:
        """DeckGenerator class should be importable."""
        from deck_generator import DeckGenerator

        assert DeckGenerator is not None

    def test_error_class_exists(self) -> None:
        """DeckGenerationError should be importable."""
        from deck_generator import DeckGenerationError

        assert issubclass(DeckGenerationError, Exception)

    def test_instantiation(self, generator: "deck_generator.DeckGenerator") -> None:
        """DeckGenerator should instantiate without errors."""
        assert generator is not None


class TestFormatPlanAsText:
    """Test the _format_plan_as_text helper."""

    def test_basic_output(self, sample_data: dict) -> None:
        """Should produce Markdown text from plan data."""
        from deck_generator import _format_plan_as_text

        text = _format_plan_as_text(sample_data)
        assert "# Recruitment Media Plan: TestCorp" in text
        assert "technology" in text
        assert "$50,000" in text
        assert "LinkedIn" in text
        assert "Indeed" in text

    def test_empty_data(self) -> None:
        """Should handle empty data without crashing."""
        from deck_generator import _format_plan_as_text

        text = _format_plan_as_text({})
        assert "# Recruitment Media Plan:" in text

    def test_none_values(self) -> None:
        """Should handle None values in data dict."""
        from deck_generator import _format_plan_as_text

        data = {
            "client_name": None,
            "industry": None,
            "budget": None,
            "channels": None,
            "roles": None,
        }
        text = _format_plan_as_text(data)
        assert "# Recruitment Media Plan:" in text

    def test_dict_roles_normalized(self) -> None:
        """Should normalize dict-format roles to strings."""
        from deck_generator import _format_plan_as_text

        data = {"roles": [{"title": "Engineer"}, {"role": "Manager"}]}
        text = _format_plan_as_text(data)
        assert "Engineer" in text
        assert "Manager" in text


class TestSlideCount:
    """Test the _slide_count helper."""

    def test_minimum_slides(self) -> None:
        """Empty data should produce minimum slide count."""
        from deck_generator import _slide_count

        count = _slide_count({})
        assert count >= 6

    def test_more_channels_more_slides(self, sample_data: dict) -> None:
        """More channels should produce more slides."""
        from deck_generator import _slide_count

        base_count = _slide_count(sample_data)
        many_channels = sample_data.copy()
        many_channels["channels"] = [{"name": f"Ch{i}"} for i in range(20)]
        high_count = _slide_count(many_channels)
        assert high_count >= base_count

    def test_max_slide_cap(self) -> None:
        """Slide count should not exceed 20."""
        from deck_generator import _slide_count

        data = {"channels": [{"name": f"Ch{i}"} for i in range(100)]}
        assert _slide_count(data) <= 20


class TestUsageTracking:
    """Test monthly usage tracking and limits."""

    def test_check_and_increment_unlimited(self) -> None:
        """Tiers without limits should always return True."""
        from deck_generator import _check_and_increment

        assert _check_and_increment("presenton") is True
        assert _check_and_increment("pptx") is True
        assert _check_and_increment("google_slides") is True

    def test_check_and_increment_with_limit(self) -> None:
        """Tiers with limits should track usage and enforce limits."""
        from deck_generator import _check_and_increment, _get_usage, _TIER_LIMITS

        # Gamma has limit of 10
        for i in range(10):
            assert _check_and_increment("gamma") is True
            assert _get_usage("gamma") == i + 1

        # 11th should fail
        assert _check_and_increment("gamma") is False

    def test_usage_resets_on_month_change(self) -> None:
        """Usage should reset when the month changes."""
        import deck_generator

        # Manually set usage
        with deck_generator._usage_lock:
            deck_generator._usage_month = "2020-01"
            deck_generator._monthly_usage = {"gamma": 100}

        # Next call should see a new month and reset
        assert deck_generator._check_and_increment("gamma") is True
        assert deck_generator._get_usage("gamma") == 1

    def test_thread_safety(self) -> None:
        """Usage tracking should be thread-safe."""
        from deck_generator import _check_and_increment, _get_usage

        errors: list[str] = []
        results: list[bool] = []

        def increment() -> None:
            try:
                result = _check_and_increment("flashdocs")
                results.append(result)
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=increment) for _ in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread safety errors: {errors}"
        assert len(results) == 100
        # All 100 should succeed (limit is 5000)
        assert all(results)
        assert _get_usage("flashdocs") == 100


class TestTierSkipping:
    """Test that tiers skip gracefully when not configured."""

    def test_presenton_skips_without_env(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """Presenton should return None when PRESENTON_URL not set."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PRESENTON_URL", None)
            result = generator._try_presenton(sample_data)
            assert result is None

    def test_gamma_skips_without_env(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """Gamma should return None when GAMMA_API_KEY not set."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GAMMA_API_KEY", None)
            result = generator._try_gamma(sample_data)
            assert result is None

    def test_magicslides_skips_without_env(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """MagicSlides should return None when MAGICSLIDES_API_KEY not set."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MAGICSLIDES_API_KEY", None)
            result = generator._try_magicslides(sample_data)
            assert result is None

    def test_google_slides_skips_without_env(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """Google Slides should return None when GOOGLE_SLIDES_CREDENTIALS not set."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GOOGLE_SLIDES_CREDENTIALS", None)
            result = generator._try_google_slides(sample_data)
            assert result is None

    def test_alai_skips_without_env(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """Alai should return None when ALAI_API_KEY not set."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ALAI_API_KEY", None)
            result = generator._try_alai(sample_data)
            assert result is None

    def test_flashdocs_skips_without_env(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """FlashDocs should return None when FLASHDOCS_API_KEY not set."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FLASHDOCS_API_KEY", None)
            result = generator._try_flashdocs(sample_data)
            assert result is None


class TestTier7PythonPptx:
    """Test that Tier 7 (python-pptx) always works."""

    def test_pptx_generates_bytes(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """Tier 7 should produce valid PPTX bytes."""
        result = generator._try_pptx(sample_data)
        assert result is not None
        assert isinstance(result, bytes)
        assert len(result) > 1000  # A real PPTX is at least a few KB
        # PPTX files are ZIP archives starting with PK header
        assert result[:2] == b"PK"

    def test_pptx_with_minimal_data(
        self, generator: "deck_generator.DeckGenerator"
    ) -> None:
        """Tier 7 should work with minimal data."""
        minimal = {"client_name": "Test", "industry": "general_entry_level"}
        result = generator._try_pptx(minimal)
        assert result is not None
        assert isinstance(result, bytes)
        assert len(result) > 1000


class TestGenerateFallback:
    """Test the full generate() fallback chain."""

    def test_generate_reaches_pptx_fallback(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """With no API keys set, generate() should fall back to python-pptx."""
        # Clear all API env vars
        env_keys = [
            "PRESENTON_URL",
            "GAMMA_API_KEY",
            "MAGICSLIDES_API_KEY",
            "GOOGLE_SLIDES_CREDENTIALS",
            "ALAI_API_KEY",
            "FLASHDOCS_API_KEY",
        ]
        with patch.dict(os.environ, {}, clear=False):
            for key in env_keys:
                os.environ.pop(key, None)

            file_bytes, provider = generator.generate(sample_data)
            assert provider == "pptx"
            assert isinstance(file_bytes, bytes)
            assert len(file_bytes) > 1000
            assert file_bytes[:2] == b"PK"

    def test_force_tier_pptx(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """force_tier='pptx' should go directly to python-pptx."""
        file_bytes, provider = generator.generate(sample_data, force_tier="pptx")
        assert provider == "pptx"
        assert isinstance(file_bytes, bytes)
        assert len(file_bytes) > 1000

    def test_force_tier_invalid(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """force_tier with invalid value should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown tier"):
            generator.generate(sample_data, force_tier="nonexistent")

    def test_force_tier_unconfigured_raises(
        self, generator: "deck_generator.DeckGenerator", sample_data: dict
    ) -> None:
        """force_tier to unconfigured tier should raise DeckGenerationError."""
        from deck_generator import DeckGenerationError

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GAMMA_API_KEY", None)
            with pytest.raises(DeckGenerationError):
                generator.generate(sample_data, force_tier="gamma")


class TestGetStatus:
    """Test the get_status() health endpoint."""

    def test_status_structure(self, generator: "deck_generator.DeckGenerator") -> None:
        """get_status() should return well-structured dict."""
        status = generator.get_status()
        assert "month" in status
        assert "tiers" in status
        assert "total_tiers" in status
        assert "available_tiers" in status
        assert status["total_tiers"] == 7

    def test_status_tier_fields(
        self, generator: "deck_generator.DeckGenerator"
    ) -> None:
        """Each tier in status should have required fields."""
        status = generator.get_status()
        for tier in status["tiers"]:
            assert "tier" in tier
            assert "name" in tier
            assert "configured" in tier
            assert "available" in tier
            assert "usage" in tier

    def test_pptx_always_available(
        self, generator: "deck_generator.DeckGenerator"
    ) -> None:
        """Tier 7 (python-pptx) should always show as available."""
        status = generator.get_status()
        pptx_tier = next(t for t in status["tiers"] if t["tier"] == "pptx")
        assert pptx_tier["configured"] is True
        assert pptx_tier["available"] is True
        assert pptx_tier["limit"] is None  # No limit

    def test_unconfigured_tiers(
        self, generator: "deck_generator.DeckGenerator"
    ) -> None:
        """Tiers without env vars should show as not configured."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GAMMA_API_KEY", None)
            status = generator.get_status()
            gamma_tier = next(t for t in status["tiers"] if t["tier"] == "gamma")
            assert gamma_tier["configured"] is False
            assert gamma_tier["available"] is False

    def test_at_least_one_available(
        self, generator: "deck_generator.DeckGenerator"
    ) -> None:
        """At least one tier (python-pptx) should always be available."""
        status = generator.get_status()
        assert status["available_tiers"] >= 1
