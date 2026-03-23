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
        """There should be exactly 29 template files."""
        html_files = list(TEMPLATES_DIR.glob("*.html"))
        assert len(html_files) == 29, (
            f"Expected 29 templates, found {len(html_files)}: "
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
