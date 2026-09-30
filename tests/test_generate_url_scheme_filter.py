"""/api/generate's URL-scheme screen rejects links, not prose (wizard audit
D-10, 2026-10-01).

Pre-fix the handler rejected ANY string field matching
``(?:javascript|file|data|vbscript)\\s*:`` -- no word boundary -- so a real
brief "Ideal candidate profile: 3+ years of MIG welding... Metadata: Q4
plant expansion." got HTTP 400 "Field 'use_case' contains a disallowed URL
scheme (javascript/file/data)" (pro-FILE:, meta-DATA:, "Big data:").
Reproduced live 2026-10-01 against f99beef.

Now app._dangerous_url_scheme only flags a scheme in URL form; tag
stripping (_sanitize_request_value) is unchanged.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import app

_REAL_BRIEF_PROSE = [
    "Ideal candidate profile: 3+ years of MIG welding. Metadata: Q4 plant expansion.",
    "Job profile: CDL-A driver, home weekly.",
    "Big data: we need 4 data engineers. Data: see attached RFP.",
    "File: RFP_2026.pdf (attached). Note: budget is flexible.",
    "Kickoff call at 10:30 CT; follow-up 14:00.",
    "Skills - JavaScript: 5+ years, TypeScript: 3 years, VBScript: legacy only.",
    "Careers site: https://careers.example.com/jobs?id=42 and http://example.org",
    "Ratio 3:1 of applicants to interviews; target CPA: $25",
    "Profile:\nSenior nurse\nMetadata:\nnone",
    "Data:image heavy campaign? No -- text ads only.",
    "mailto:recruiting@example.com or tel:+1-555-0100",
]

_LINKS = [
    ("javascript:alert(1)", "javascript"),
    ("Click javascript:void(0) now", "javascript"),
    ("JaVaScRiPt:alert(document.cookie)", "javascript"),
    ('href="javascript: alert(1)"', "javascript"),
    ("(javascript:alert(1))", "javascript"),
    ("vbscript:msgbox(1)", "vbscript"),
    ("data:text/html;base64,PHNjcmlwdD5hbGVydCgxKTwvc2NyaXB0Pg==", "data"),
    ("see data:image/svg+xml,<svg onload=alert(1)>", "data"),
    ("file:///etc/passwd", "file"),
    ("open FILE://server/share/x", "file"),
]


@pytest.mark.parametrize("text", _REAL_BRIEF_PROSE)
@pytest.mark.parametrize("field", ["use_case", "call_transcript", "target_demographic"])
def test_prose_with_colons_is_not_a_link(field, text):
    assert app._dangerous_url_scheme(field, text) == ""


@pytest.mark.parametrize("text,scheme", _LINKS)
def test_real_dangerous_links_are_still_rejected(text, scheme):
    assert app._dangerous_url_scheme("use_case", text) == scheme


@pytest.mark.parametrize(
    "value,scheme",
    [
        ("javascript: alert(1)", "javascript"),
        (" java\tscript:alert(1)", "javascript"),
        ("DATA:text/plain,hi", "data"),
        ("file:c:/x", "file"),
    ],
)
def test_url_fields_reject_any_value_starting_with_a_scheme(value, scheme):
    assert app._dangerous_url_scheme("client_website", value) == scheme


def test_url_field_with_a_normal_website_passes():
    assert app._dangerous_url_scheme("client_website", "https://acme.com/careers") == ""


def test_tag_stripping_is_unchanged():
    assert (
        app._sanitize_request_value("<script>alert(1)</script>profile: welder")
        == "alert(1)profile: welder"
    )


def test_handler_uses_the_shared_screen_not_the_old_pattern():
    src = (Path(__file__).resolve().parent.parent / "app.py").read_text(encoding="utf-8")
    start = src.index("# ── Gold Standard: Reject javascript:/vbscript:/data:/file: URLs ──")
    block = src[start : src.index("_validation_warnings = []", start)]
    assert "_dangerous_url_scheme(_fkey, _item)" in block
    assert r"(?:javascript|file|data|vbscript)\s*:" not in src
    assert "field=str(_fkey)" in block
