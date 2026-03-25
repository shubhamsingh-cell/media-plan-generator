"""Template validation tests for all HTML files in templates/."""

import re
from pathlib import Path

import pytest

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"

EXPECTED_TEMPLATES = [
    "ab-testing.html",
    "api-portal.html",
    "applyflow-demo.html",
    "audit.html",
    "competitive.html",
    "compliance-guard.html",
    "creative-ai.html",
    "dashboard.html",
    "hire-signal.html",
    "hub.html",
    "index.html",
    "market-intel.html",
    "market-pulse.html",
    "nova.html",
    "observability.html",
    "payscale-sync.html",
    "post-campaign.html",
    "quick-brief.html",
    "quick-plan.html",
    "roi-calculator.html",
    "simulator.html",
    "skill-target.html",
    "social-plan.html",
    "talent-heatmap.html",
    "tracker.html",
    "vendor-iq.html",
    "pricing.html",
    "privacy.html",
    "terms.html",
]


class TestTemplateExistence:
    """All expected template files must exist."""

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_template_exists(self, filename: str) -> None:
        """Each expected template file should exist in templates/."""
        path = TEMPLATES_DIR / filename
        assert path.exists(), f"Missing template: {filename}"

    def test_template_count(self) -> None:
        """There should be at least as many templates as EXPECTED_TEMPLATES."""
        html_files = list(TEMPLATES_DIR.glob("*.html"))
        assert len(html_files) >= len(EXPECTED_TEMPLATES), (
            f"Expected at least {len(EXPECTED_TEMPLATES)} templates, found {len(html_files)}: "
            f"{[f.name for f in html_files]}"
        )


class TestTemplateStructure:
    """Each template must have valid HTML structure."""

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_has_doctype(self, filename: str) -> None:
        """Each template should have a DOCTYPE declaration."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8").lower()
        assert "<!doctype" in content, f"{filename} missing <!DOCTYPE>"

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_has_html_tag(self, filename: str) -> None:
        """Each template should have an <html> tag."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8").lower()
        assert "<html" in content, f"{filename} missing <html>"

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_has_head_tag(self, filename: str) -> None:
        """Each template should have a <head> tag."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8").lower()
        assert "<head" in content, f"{filename} missing <head>"

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_has_body_tag(self, filename: str) -> None:
        """Each template should have a <body> tag."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8").lower()
        assert "<body" in content, f"{filename} missing <body>"


class TestAccessibility:
    """Accessibility requirements for all templates."""

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_prefers_reduced_motion(self, filename: str) -> None:
        """Each template must include a prefers-reduced-motion media query."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8")
        assert (
            "prefers-reduced-motion" in content
        ), f"{filename} missing prefers-reduced-motion media query"


_TEMPLATES_WITH_FOOTER = [
    t
    for t in EXPECTED_TEMPLATES
    if t
    not in {
        "api-portal.html",
        "dashboard.html",
        "hire-signal.html",
        "market-intel.html",
        "nova.html",
        "observability.html",
        "social-plan.html",
        "talent-heatmap.html",
    }
]


class TestBranding:
    """Brand consistency checks across all templates."""

    @pytest.mark.parametrize("filename", _TEMPLATES_WITH_FOOTER)
    def test_linkedin_url(self, filename: str) -> None:
        """Templates with footers must contain the correct LinkedIn URL."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8")
        assert (
            "/in/chandel13/" in content
        ), f"{filename} missing LinkedIn URL /in/chandel13/"

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_text_color(self, filename: str) -> None:
        """Each template should use the standard text color #d4d4d8 or #e4e4e7."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8").lower()
        has_color = "#d4d4d8" in content or "#e4e4e7" in content
        assert has_color, f"{filename} missing standard text color (#d4d4d8 or #e4e4e7)"


class TestSecurityInTemplates:
    """Templates must not leak sensitive information."""

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_no_hardcoded_admin_key(self, filename: str) -> None:
        """No template should contain a hardcoded admin key value."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8")
        # Check for the known admin key as a default/hardcoded value
        # (references in JS variable names or input placeholders are acceptable)
        assert (
            '"Chandel13"' not in content and "'Chandel13'" not in content
        ), f"{filename} contains hardcoded admin key 'Chandel13'"

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_no_hardcoded_api_keys(self, filename: str) -> None:
        """No template should contain hardcoded API keys."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8")
        # Check for common API key patterns (sk-xxx, phc_xxx, etc.)
        # Allow PostHog project token in templates (it's public by design)
        suspicious_patterns = [
            "sk-ant-",  # Anthropic keys
            "sk-proj-",  # OpenAI keys
            "gsk_",  # Groq keys
        ]
        for pattern in suspicious_patterns:
            assert (
                pattern not in content
            ), f"{filename} may contain hardcoded API key (found '{pattern}')"


# ═══════════════════════════════════════════════════════════════════════════════
# Brand Color Validation
# ═══════════════════════════════════════════════════════════════════════════════

# Brand colors: PORT_GORE=#202058, BLUE_VIOLET=#5A54BD, DOWNY_TEAL=#6BB3CD
_BRAND_COLORS = {"#202058", "#5a54bd", "#6bb3cd"}
# Off-brand colors that should NOT appear
_OFF_BRAND_COLORS = {"#4f46e5", "#6366f1", "#3b82f6", "#8b5cf6"}


class TestBrandColors:
    """Validate brand color consistency across templates."""

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_has_brand_color(self, filename: str) -> None:
        """Each template should use at least one brand color."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8").lower()
        has_brand = any(color in content for color in _BRAND_COLORS)
        assert (
            has_brand
        ), f"{filename} missing brand colors ({', '.join(_BRAND_COLORS)})"


# ═══════════════════════════════════════════════════════════════════════════════
# Title Tag Validation
# ═══════════════════════════════════════════════════════════════════════════════


class TestTitleTags:
    """Validate title tags are present and meaningful."""

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_has_title_tag(self, filename: str) -> None:
        """Each template should have a <title> tag."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8").lower()
        assert (
            "<title>" in content and "</title>" in content
        ), f"{filename} missing <title> tag"

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_title_not_empty(self, filename: str) -> None:
        """Title tag should not be empty."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8")
        match = re.search(
            r"<title[^>]*>(.*?)</title>", content, re.IGNORECASE | re.DOTALL
        )
        assert match is not None, f"{filename} missing <title>"
        title = match.group(1).strip()
        assert len(title) > 0, f"{filename} has empty <title>"


# ═══════════════════════════════════════════════════════════════════════════════
# Meta Tags (SEO/OG)
# ═══════════════════════════════════════════════════════════════════════════════


class TestMetaTags:
    """Validate meta tags for SEO and social sharing."""

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_has_viewport_meta(self, filename: str) -> None:
        """Each template should have a viewport meta tag for mobile."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8").lower()
        assert "viewport" in content, f"{filename} missing viewport meta tag"

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_has_charset_meta(self, filename: str) -> None:
        """Each template should declare UTF-8 charset."""
        content = (TEMPLATES_DIR / filename).read_text(encoding="utf-8").lower()
        assert "utf-8" in content, f"{filename} missing UTF-8 charset declaration"


# ═══════════════════════════════════════════════════════════════════════════════
# Template File Size
# ═══════════════════════════════════════════════════════════════════════════════


class TestTemplateSize:
    """Validate template file sizes are reasonable."""

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_not_empty(self, filename: str) -> None:
        """Template files should not be empty."""
        path = TEMPLATES_DIR / filename
        assert path.stat().st_size > 0, f"{filename} is empty"

    @pytest.mark.parametrize("filename", EXPECTED_TEMPLATES)
    def test_not_oversized(self, filename: str) -> None:
        """Template files should not exceed 500KB (inline CSS/JS can be large)."""
        path = TEMPLATES_DIR / filename
        size_kb = path.stat().st_size / 1024
        assert size_kb < 500, f"{filename} is {size_kb:.0f}KB, exceeds 500KB limit"
