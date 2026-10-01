"""Generated files always carry a usable client name (delta verifier on
5a59c1b, 2026-10-01).

Non-Latin client names are accepted now (one letter or digit in any
script), but every filename builder kept only [a-zA-Z0-9_-] and stripped the
rest, so "朝日新聞" produced "_Media_Plan_Bundle.zip", "_Media_Plan.xlsx" and
"_Strategy_Deck.pptx". app._client_file_slug folds accents, falls back to
"Client", and is the only builder.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import app


@pytest.mark.parametrize(
    "name,slug",
    [
        ("朝日新聞", "Client"),
        ("Яндекс", "Client"),
        ("😀", "Client"),
        ("!!!", "Client"),
        ("", "Client"),
        (None, "Client"),
        ("!!!Acme!!!", "Acme"),
        ("朝日 Asahi Shimbun", "Asahi_Shimbun"),
        ("Société Générale", "Societe_Generale"),
        ("Café Zoë & Sons", "Cafe_Zoe_Sons"),
        ("H&M", "H_M"),
        ("3M", "3M"),
        ("Acme Corp", "Acme_Corp"),
    ],
)
def test_slug_is_never_empty(name, slug):
    assert app._client_file_slug(name) == slug


def test_slug_respects_the_client_name_limit():
    assert len(app._client_file_slug("A" * 500)) == 200


def test_every_file_name_builder_uses_the_slug():
    src = (Path(__file__).resolve().parent.parent / "app.py").read_text(encoding="utf-8")
    body = src[src.index("def _client_file_slug(") :]
    body = body[body.index("\ndef ") :]  # everything after the helper itself
    # no other client-name sanitiser left anywhere in app.py
    assert not re.search(r're\.sub\(\s*r"\[\^a-zA-Z0-9_\\-\]"', body)
    assert src.count("_client_file_slug(") == 1 + 4  # def + async, sync, 2 saved copies
