"""A platform a market's government blocks never reaches a plan for that market.

TikTok has been blocked in India since 2020-06-29 (data/platform_market_blocks.json
carries the source and the date it was checked).  ``publisher_filter`` reads the
file lazily, caches it under a lock and drops the cache in ``reset_caches``;
every surface that lists ad platforms asks it, so this file covers the mechanism
and the recommender, Nova tool and wizard hint that read it.  The workbook and
deck are covered with the rest of the plan numbers in
``test_intl_plan_number_honesty.py``.
"""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
for _n in (
    "anthropic",
    "openai",
    "supabase",
    "redis",
    "qdrant_client",
    "sentence_transformers",
    "posthog",
):
    sys.modules.setdefault(_n, types.ModuleType(_n))

import channel_recommender  # noqa: E402
import publisher_filter  # noqa: E402


def test_market_block_data_is_sourced():
    raw = json.loads(
        (PROJECT_ROOT / "data" / "platform_market_blocks.json").read_text(
            encoding="utf-8"
        )
    )
    assert raw["blocks"]
    for block in raw["blocks"]:
        assert str(block.get("source") or "").startswith("http"), block
        assert str(block.get("checked_on") or "").strip(), block


def test_tiktok_does_not_run_in_india_only():
    for name in ("TikTok Ads", "TikTok", "tiktok.com"):
        assert not publisher_filter.platform_runs_in(name, "India"), name
        assert not publisher_filter.platform_runs_in(name, "IN"), name
        assert publisher_filter.platform_runs_in(name, "United States"), name
        assert publisher_filter.platform_runs_in(name, "Vietnam"), name
        assert publisher_filter.platform_runs_in(name, None), name
    assert publisher_filter.platform_runs_in("Meta (Facebook/Instagram)", "India")
    # a compound is blocked only when every part is
    assert publisher_filter.platform_runs_in("LinkedIn / TikTok Ads", "India")
    assert not publisher_filter.platform_runs_in("TikTok Ads / TikTok", "India")


def _platforms(rec: dict) -> List[str]:
    return [
        c["platform"]
        for tier in ("must_have", "should_have", "test_and_learn", "skip")
        for c in rec[tier]
    ]


def test_nova_channel_recommender_drops_tiktok_for_india():
    india = channel_recommender.recommend_channels(
        "general_entry_level", "Agent", 100_000, locations=["Bengaluru, India"]
    )
    assert "TikTok Ads" not in _platforms(india)
    us = channel_recommender.recommend_channels(
        "general_entry_level", "Agent", 100_000, locations=["Dallas, TX"]
    )
    assert "TikTok Ads" in _platforms(us)
    nolocs = channel_recommender.recommend_channels(
        "general_entry_level", "Agent", 100_000
    )
    assert "TikTok Ads" in _platforms(nolocs)


def test_research_prose_naming_tiktok_leaves_an_india_only_plan():
    text = "Tiktok Usage: 68% use TikTok; Snapchat Usage: 67% use Snapchat"
    india = publisher_filter.sweep_text(
        text, {"locations": ["Bengaluru, India"]}, ["India"]
    )
    assert "TikTok" not in india and "Snapchat Usage: 67%" in india
    # a US plan, and a plan with a US market beside India, keep the sentence
    us = publisher_filter.sweep_text(
        text, {"locations": ["Dallas, TX"]}, ["United States"]
    )
    assert us == text
    both = publisher_filter.sweep_text(
        text,
        {"locations": ["Dallas, TX", "Bengaluru, India"]},
        ["United States", "India"],
    )
    assert both == text


def test_blocked_platform_cache_is_dropped_by_reset_caches():
    publisher_filter.reset_caches()
    assert publisher_filter._blocks_cache is None
    assert not publisher_filter.platform_runs_in("TikTok Ads", "India")
    assert publisher_filter._blocks_cache  # loaded lazily on first use
    publisher_filter.reset_caches()
    assert publisher_filter._blocks_cache is None
    assert publisher_filter.platform_runs_in("TikTok Ads", "Vietnam")


def test_unreadable_block_file_fails_open_and_is_not_cached_forever(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        publisher_filter, "_BLOCKS_PATH", str(tmp_path / "missing.json")
    )
    publisher_filter.reset_caches()
    try:
        # no data: nothing is blocked (a recommender must never fail on this)
        assert publisher_filter.platform_runs_in("TikTok Ads", "India")
    finally:
        monkeypatch.undo()
        publisher_filter.reset_caches()
    assert not publisher_filter.platform_runs_in("TikTok Ads", "India")


def test_a_failed_read_is_retried_on_the_next_call_without_a_reset(
    monkeypatch, tmp_path
):
    """A transient read failure must not switch blocking off until a restart:
    nothing is cached, and the next call (no reset_caches) reads the file."""
    real = publisher_filter._BLOCKS_PATH
    publisher_filter.reset_caches()
    monkeypatch.setattr(
        publisher_filter, "_BLOCKS_PATH", str(tmp_path / "missing.json")
    )
    assert publisher_filter.platform_runs_in("TikTok Ads", "India")
    assert publisher_filter._blocks_cache is None
    monkeypatch.setattr(publisher_filter, "_BLOCKS_PATH", real)
    assert not publisher_filter.platform_runs_in("TikTok Ads", "India")
    assert publisher_filter._blocks_cache


def test_block_keys_are_built_like_every_lookup(monkeypatch):
    """Block keys go through publisher_filter._keys_for (the alias groups of
    publisher_availability.json), the function lookups use: a block listing
    one name of an alias group blocks every name of that group."""
    publisher_filter.reset_caches()
    blocks = publisher_filter._load_platform_blocks()
    tiktok = next(b for b in blocks if b["name"] == "TikTok")
    expected = {
        k
        for n in [tiktok["name"]] + list(tiktok.get("aliases") or [])
        for k in publisher_filter._keys_for(n)
    }
    assert tiktok["_keys"] == frozenset(expected)
    # an alias group whose canonical key is not the block's normalized name:
    # lookups resolve "TikTok" to the group key, so the block must too
    real_load = publisher_filter._load_availability
    avail = dict(real_load())
    avail["group_of"] = dict(avail["group_of"], tiktok="shortvideogroup")
    monkeypatch.setattr(publisher_filter, "_load_availability", lambda: avail)
    publisher_filter.reset_caches()
    assert publisher_filter._keys_for("TikTok") == ["shortvideogroup"]
    assert not publisher_filter.platform_runs_in("TikTok", "India")
    monkeypatch.setattr(publisher_filter, "_load_availability", real_load)
    publisher_filter.reset_caches()


def test_block_lookup_is_thread_safe_under_reset():
    import threading

    errors: List[BaseException] = []

    def hammer() -> None:
        try:
            for _ in range(200):
                assert not publisher_filter.platform_runs_in("TikTok", "India")
                publisher_filter.reset_caches()
        except BaseException as exc:  # noqa: BLE001 -- surface any thread failure
            errors.append(exc)

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not errors, errors


def test_nova_channel_tool_drops_tiktok_for_india_and_keeps_it_for_the_us():
    from nova import Nova

    nova_ = Nova.__new__(Nova)
    nova_._data_cache = {}
    args = {"industry": "general_entry_level", "role": "Agent", "budget": 100_000}
    india = nova_._recommend_channels_tool(dict(args, locations=["Bengaluru, India"]))
    us = nova_._recommend_channels_tool(dict(args, locations=["Dallas, TX"]))
    assert "TikTok" not in json.dumps(india, default=str)
    assert "TikTok" in json.dumps(us, default=str)


def test_wizard_channel_hints_name_tiktok_only_where_it_runs():
    import app

    def reasons(*locs: str) -> str:
        found = app._copilot_suggest_channels(
            "retail_consumer", ["Retail Associate"], list(locs), "", ""
        )
        return " | ".join(str(h.get("reason")) for h in found)

    assert "TikTok" in reasons("Dallas, TX")
    assert "TikTok" not in reasons("Mumbai, India")
    assert "Instagram reaches Gen-Z retail candidates" in reasons("Mumbai, India")
    assert "TikTok" in reasons("Dallas, TX", "Mumbai, India")  # a US market keeps it
