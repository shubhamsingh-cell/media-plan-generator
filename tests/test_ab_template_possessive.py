"""The A/B template's variant-B description builds its possessive through
display_format.possessive() (2026-10-17) -- the last bare f"{company}'s" in
app.py after the deck/workbook sweep (cb9c857, f3f9d31). "McDonald's's" was
the client-visible symptom."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module  # noqa: E402
import display_format  # noqa: E402


@pytest.mark.parametrize(
    "company, expected",
    [
        ("McDonald's", "McDonald's"),
        ("Werner Enterprises", "Werner Enterprises'"),
        ("Barclays", "Barclays'"),
        ("Joveo", "Joveo's"),
        ("3M", "3M's"),
    ],
)
def test_variant_b_description_uses_the_shared_possessive(company, expected):
    result = app_module._generate_ab_test_template(
        "Registered Nurse", company, "healthcare_medical", "active", "indeed", 5000.0
    )
    description = result["variant_b"]["description"]
    assert description.startswith(f"Be part of {expected} "), description
    assert "'s's" not in description
    assert f"{company}'s" not in description or expected == f"{company}'s"
    # Parity with the helper the deck and workbook use.
    assert expected == display_format.possessive(company)


def test_variant_b_falls_back_to_the_bare_form_without_display_format(monkeypatch):
    monkeypatch.setattr(app_module, "display_format", None)
    result = app_module._generate_ab_test_template(
        "Registered Nurse", "Joveo", "healthcare_medical", "active", "indeed", 5000.0
    )
    assert result["variant_b"]["description"].startswith("Be part of Joveo's ")
