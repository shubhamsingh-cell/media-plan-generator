"""scripts/seed_supabase.py seeds the router's Sonnet model id, not a literal."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


def test_seeded_default_model_is_the_router_sonnet_tier(monkeypatch) -> None:
    import llm_router
    import seed_supabase

    captured: dict = {}

    def _fake_upsert(_client, _table, rows, dry_run=False):
        captured["rows"] = rows
        return len(rows)

    monkeypatch.setattr(seed_supabase, "_upsert_rows", _fake_upsert)
    seed_supabase.seed_nova_memory(object(), dry_run=True)
    row = next(r for r in captured["rows"] if r["key"] == "default_model")
    expected = llm_router.PROVIDER_CONFIG[llm_router.CLAUDE]["model"]
    assert row["value"] == {"model": expected}
    assert row["content"] == expected
    assert "claude-sonnet-4-20250514" not in (ROOT / "scripts/seed_supabase.py").read_text()
