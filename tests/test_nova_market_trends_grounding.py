"""Tests for Nova's query_market_trends citation trail + honest provenance.

Background: ``Nova._query_market_trends`` injects per-channel CPC benchmarks
from ``data/channel_benchmarks_live.json`` into the tool result (v4). Before
this test, the injection carried only ``cpc_range``/``cpa_range``/``model``/
``last_updated`` -- dropping each entry's ``notes`` and ``sources``, which is
where the actual citation (e.g. "Pin.com, aggregator-inferred") lives. The
grounding doctrine's whole point is that a claim traces to a source Nova can
quote; silently dropping the sources broke that for this tool.

Separately, the result hardcoded ``live_benchmarks_source =
"channel_benchmarks_live.json"``, and the tool description called the data
"live channel CPC benchmarks" -- both imply real-time freshness that doesn't
exist (the file is a static, dated web-research snapshot; the refresh daemon
that would make it live is disabled). The fix reads the file's own
``_provenance`` string (falling back to the filename only if absent) and adds
a ``benchmarks_vintage`` field (max per-entry ``last_updated``).

These tests point ``nova.DATA_DIR`` at a temp directory with a synthetic
``channel_benchmarks_live.json`` fixture so they're deterministic and don't
depend on the real (gitignored) runtime file's contents. Follows the
``Nova.__new__(Nova)`` no-heavy-init pattern from tests/test_nova_keystone.py.

Runs under pytest, or standalone: ``python3 tests/test_nova_market_trends_grounding.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import nova  # noqa: E402

TOOL = "query_market_trends"

_LONG_NOTE = "x" * 1000  # exceeds the 800-char defensive cap


def _nova():
    """A real Nova instance without the heavy __init__ (KB load)."""
    return nova.Nova.__new__(nova.Nova)


def _write_fixture(tmp_path: Path) -> None:
    fixture = {
        "data": [
            {
                "channel": "indeed",
                "metadata": {
                    "cpc_range": {"min": 0.97, "max": 2.71, "currency": "USD"},
                    "model": "CPC auction (Sponsored Jobs)",
                    "last_updated": "2026-07-16",
                    "notes": _LONG_NOTE,
                    "sources": [
                        "https://www.indeed.com/hire/cs/pricing",
                        "https://www.pin.com/blog/indeed-pricing/",
                    ],
                },
            },
            {
                # Subscription-only board -- no cpc_range, but still carries
                # a citation trail that must survive the injection.
                "channel": "ziprecruiter",
                "metadata": {
                    "model": "Flat monthly subscription per job slot",
                    "last_updated": "2026-06-01",
                    "notes": "ZipRecruiter sells subscriptions, not CPC.",
                    "sources": ["https://www.ziprecruiter.com/plans"],
                },
            },
        ],
        "_refreshed_at": 1784186961.39,
        "_provenance": "TEST: web-researched snapshot, 2026-07-16",
    }
    (tmp_path / "channel_benchmarks_live.json").write_text(json.dumps(fixture))


def test_notes_and_sources_included_for_indeed(tmp_path, monkeypatch):
    _write_fixture(tmp_path)
    monkeypatch.setattr(nova, "DATA_DIR", tmp_path)

    out = _nova()._query_market_trends({})

    boards = out.get("live_channel_benchmarks") or {}
    assert "indeed" in boards
    indeed = boards["indeed"]
    assert indeed["sources"] == [
        "https://www.indeed.com/hire/cs/pricing",
        "https://www.pin.com/blog/indeed-pricing/",
    ]
    # Notes present and capped at 800 chars (defensive bound), not silently
    # dropped as before.
    assert indeed["notes"]
    assert len(indeed["notes"]) <= 800


def test_notes_and_sources_included_for_board_without_cpc_range(tmp_path, monkeypatch):
    _write_fixture(tmp_path)
    monkeypatch.setattr(nova, "DATA_DIR", tmp_path)

    out = _nova()._query_market_trends({})

    boards = out.get("live_channel_benchmarks") or {}
    assert "ziprecruiter" in boards
    zr = boards["ziprecruiter"]
    assert zr["sources"] == ["https://www.ziprecruiter.com/plans"]
    assert "subscriptions" in zr["notes"]
    assert zr["cpc_range"] == {}


def test_honest_provenance_label_reads_file_provenance(tmp_path, monkeypatch):
    _write_fixture(tmp_path)
    monkeypatch.setattr(nova, "DATA_DIR", tmp_path)

    out = _nova()._query_market_trends({})

    # Provenance is the file's own honest label, NOT the hardcoded filename.
    assert out["live_benchmarks_source"] == "TEST: web-researched snapshot, 2026-07-16"
    assert out["live_benchmarks_source"] != "channel_benchmarks_live.json"


def test_benchmarks_vintage_is_max_last_updated(tmp_path, monkeypatch):
    _write_fixture(tmp_path)
    monkeypatch.setattr(nova, "DATA_DIR", tmp_path)

    out = _nova()._query_market_trends({})

    # indeed=2026-07-16, ziprecruiter=2026-06-01 -> max is 2026-07-16.
    assert out["benchmarks_vintage"] == "2026-07-16"


def test_provenance_falls_back_to_filename_when_absent(tmp_path, monkeypatch):
    fixture = {
        "data": [
            {
                "channel": "indeed",
                "metadata": {
                    "cpc_range": {"min": 0.97, "max": 2.71},
                    "last_updated": "2026-07-16",
                },
            },
        ],
        # No "_provenance" key.
    }
    (tmp_path / "channel_benchmarks_live.json").write_text(json.dumps(fixture))
    monkeypatch.setattr(nova, "DATA_DIR", tmp_path)

    out = _nova()._query_market_trends({})
    assert out["live_benchmarks_source"] == "channel_benchmarks_live.json"


# ── registration / description wiring ───────────────────────────────────────
def test_tool_description_no_longer_claims_live_freshness():
    defs = _nova().get_tool_definitions()
    by_name = {d["name"]: d for d in defs}
    assert TOOL in by_name
    desc = by_name[TOOL]["description"]
    # Old wording implied real-time freshness; new wording is honest about
    # the data being a dated, web-researched baseline.
    assert "web-researched baseline" in desc
    # Trigger phrasing must survive the honesty edit.
    assert "CPC trends" in desc
    assert "cheapest time to advertise" in desc


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
