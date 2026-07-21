"""Repo-wide like-for-like drift pins for hardcoded Indeed/LinkedIn CPCs.

Every site below carries a hardcoded recruitment-CPC figure that is supposed
to track benchmark_registry.CHANNEL_BENCHMARKS -- the repo's cited, refreshed
source (July-2026 research: Indeed $0.97-$2.71 typical band, median 1.62;
LinkedIn Promoted-Jobs $1.50-$4.50 band, median 2.60). Before these pins,
every registry refresh left stragglers behind: the July-2026 reconciliation
took three sessions and a week of repeated sweeps to hunt down 0.50/0.92/5.26
copies, and the last sweep still found live drift (campaign_optimizer 6.50,
quick_plan 3.80, seed_supply_repository 5.26).

If the registry refreshes again, these tests fail and point at the exact
dict to update. Update the DICT with the newly cited figure -- never relax a
pin to match a drifted dict.

data_synthesizer's _PLATFORM_BENCHMARKS has its own pin file
(tests/test_platform_benchmark_fallback.py).

Runs under pytest, or standalone:
``python3 tests/test_cpc_fallback_drift_pins.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import benchmark_registry  # noqa: E402

_REGISTRY_INDEED = benchmark_registry.CHANNEL_BENCHMARKS["indeed"]["cpc"]
_REGISTRY_LINKEDIN = benchmark_registry.CHANNEL_BENCHMARKS["linkedin"]["cpc"]

# Figures retired by the 2026-07-16 reconciliation. The registry itself must
# never regress to one of these (a refresh to a NEW cited figure is fine).
_RETIRED = {0.50, 0.92, 5.26}


def test_registry_itself_not_regressed_to_retired_values():
    assert _REGISTRY_INDEED not in _RETIRED, (
        f"CHANNEL_BENCHMARKS indeed cpc {_REGISTRY_INDEED} is a RETIRED "
        "figure -- the 2026-07-16 reconciliation must not be reverted"
    )
    assert _REGISTRY_LINKEDIN not in _RETIRED, (
        f"CHANNEL_BENCHMARKS linkedin cpc {_REGISTRY_LINKEDIN} is a RETIRED "
        "figure -- the 2026-07-16 reconciliation must not be reverted"
    )


def test_audit_tool_fallback_dict_pins():
    import audit_tool

    fb = audit_tool._FALLBACK_BENCHMARKS
    assert fb["indeed"]["cpc"] == _REGISTRY_INDEED, (
        f"audit_tool._FALLBACK_BENCHMARKS indeed cpc {fb['indeed']['cpc']} "
        f"drifted from registry {_REGISTRY_INDEED}"
    )
    assert fb["linkedin"]["cpc"] == _REGISTRY_LINKEDIN, (
        f"audit_tool._FALLBACK_BENCHMARKS linkedin cpc "
        f"{fb['linkedin']['cpc']} drifted from registry {_REGISTRY_LINKEDIN}"
    )


def test_audit_tool_fallback_path_behavioral(monkeypatch):
    import audit_tool

    monkeypatch.setattr(audit_tool, "_HAS_BENCHMARK_REGISTRY", False)
    assert audit_tool._get_fallback("indeed", "cpc") == _REGISTRY_INDEED
    assert audit_tool._get_fallback("linkedin", "cpc") == _REGISTRY_LINKEDIN


def test_performance_tracker_fallback_behavioral(monkeypatch):
    """The dict is function-local; pin it through the actual fallback path."""
    import performance_tracker

    monkeypatch.setattr(performance_tracker, "_HAS_BENCHMARK_REGISTRY", False)
    got_indeed = performance_tracker._fallback_benchmark("indeed", "cpc")
    got_linkedin = performance_tracker._fallback_benchmark("linkedin", "cpc")
    assert got_indeed == _REGISTRY_INDEED, (
        f"performance_tracker fallback indeed cpc {got_indeed} drifted from "
        f"registry {_REGISTRY_INDEED} -- update the _fallbacks dict inside "
        "_fallback_benchmark"
    )
    assert got_linkedin == _REGISTRY_LINKEDIN, (
        f"performance_tracker fallback linkedin cpc {got_linkedin} drifted "
        f"from registry {_REGISTRY_LINKEDIN} -- update the _fallbacks dict "
        "inside _fallback_benchmark"
    )


def test_competitive_intel_fallback_behavioral(monkeypatch):
    import competitive_intel

    monkeypatch.setattr(competitive_intel, "_HAS_BENCHMARK_REGISTRY", False)
    # "overall" is not in the industry-multiplier table -> multiplier 1.0,
    # so the raw base-dict values surface unscaled.
    result = competitive_intel._fallback_benchmarks("overall")
    assert result["indeed"]["cpc"] == _REGISTRY_INDEED, (
        f"competitive_intel fallback indeed cpc {result['indeed']['cpc']} "
        f"drifted from registry {_REGISTRY_INDEED}"
    )
    assert result["linkedin"]["cpc"] == _REGISTRY_LINKEDIN, (
        f"competitive_intel fallback linkedin cpc "
        f"{result['linkedin']['cpc']} drifted from registry "
        f"{_REGISTRY_LINKEDIN}"
    )


def test_market_intel_reports_import_error_fallback_pins():
    import market_intel_reports

    fb = market_intel_reports._PLATFORM_BENCHMARKS_FALLBACK
    assert fb["indeed"]["cpc"] == _REGISTRY_INDEED, (
        f"market_intel_reports fallback indeed cpc {fb['indeed']['cpc']} "
        f"drifted from registry {_REGISTRY_INDEED}"
    )
    assert fb["linkedin"]["cpc"] == _REGISTRY_LINKEDIN, (
        f"market_intel_reports fallback linkedin cpc "
        f"{fb['linkedin']['cpc']} drifted from registry {_REGISTRY_LINKEDIN}"
    )


def test_roi_projector_channels_pins():
    import roi_projector

    assert roi_projector._CHANNELS["indeed"][0] == _REGISTRY_INDEED, (
        f"roi_projector._CHANNELS indeed cpc "
        f"{roi_projector._CHANNELS['indeed'][0]} drifted from registry "
        f"{_REGISTRY_INDEED}"
    )
    assert roi_projector._CHANNELS["linkedin"][0] == _REGISTRY_LINKEDIN, (
        f"roi_projector._CHANNELS linkedin cpc "
        f"{roi_projector._CHANNELS['linkedin'][0]} drifted from registry "
        f"{_REGISTRY_LINKEDIN}"
    )
    # The generic "job boards" alias tracks Indeed (the reference major board).
    assert roi_projector._CHANNELS["job boards"][0] == _REGISTRY_INDEED, (
        "roi_projector._CHANNELS 'job boards' alias drifted from the Indeed "
        f"registry figure {_REGISTRY_INDEED}"
    )


def test_market_signals_cpc_benchmark_medians_pin():
    import market_signals

    ms = market_signals._PLATFORM_CPC_BENCHMARKS
    assert ms["indeed"]["median"] == _REGISTRY_INDEED, (
        f"market_signals indeed median {ms['indeed']['median']} drifted "
        f"from registry {_REGISTRY_INDEED}"
    )
    assert ms["linkedin"]["median"] == _REGISTRY_LINKEDIN, (
        f"market_signals linkedin median {ms['linkedin']['median']} drifted "
        f"from registry {_REGISTRY_LINKEDIN}"
    )


def test_campaign_optimizer_base_cpc_pins():
    import campaign_optimizer

    assert campaign_optimizer._CH["indeed"][2] == _REGISTRY_INDEED, (
        f"campaign_optimizer._CH indeed base CPC "
        f"{campaign_optimizer._CH['indeed'][2]} drifted from registry "
        f"{_REGISTRY_INDEED}"
    )
    assert campaign_optimizer._CH["linkedin"][2] == _REGISTRY_LINKEDIN, (
        f"campaign_optimizer._CH linkedin base CPC "
        f"{campaign_optimizer._CH['linkedin'][2]} drifted from registry "
        f"{_REGISTRY_LINKEDIN} (6.50 lived here until 2026-07-21, biasing "
        "the optimizer against LinkedIn)"
    )


def test_quick_plan_base_cpc_pins():
    import quick_plan

    assert quick_plan._BASE_CPC_MAP["indeed"] == _REGISTRY_INDEED, (
        f"quick_plan._BASE_CPC_MAP indeed {quick_plan._BASE_CPC_MAP['indeed']} "
        f"drifted from registry {_REGISTRY_INDEED}"
    )
    assert quick_plan._BASE_CPC_MAP["linkedin"] == _REGISTRY_LINKEDIN, (
        f"quick_plan._BASE_CPC_MAP linkedin "
        f"{quick_plan._BASE_CPC_MAP['linkedin']} drifted from registry "
        f"{_REGISTRY_LINKEDIN}"
    )


def test_budget_simulator_fallback_cpc_pins():
    import budget_simulator

    assert budget_simulator._FALLBACK_CPC["linkedin"] == _REGISTRY_LINKEDIN, (
        f"budget_simulator._FALLBACK_CPC linkedin "
        f"{budget_simulator._FALLBACK_CPC['linkedin']} drifted from registry "
        f"{_REGISTRY_LINKEDIN}"
    )
    # The generic job_boards bucket tracks Indeed, matching roi_projector's
    # "job boards" alias convention.
    assert budget_simulator._FALLBACK_CPC["job_boards"] == _REGISTRY_INDEED, (
        f"budget_simulator._FALLBACK_CPC job_boards "
        f"{budget_simulator._FALLBACK_CPC['job_boards']} drifted from the "
        f"Indeed registry figure {_REGISTRY_INDEED}"
    )


def test_seed_supply_repository_json_pins():
    rows = json.loads(
        (PROJECT_ROOT / "data" / "seed_supply_repository.json").read_text()
    )
    by_name = {r["name"]: r for r in rows}
    indeed_cpc = by_name["Indeed"]["performance"]["avg_cpc"]
    linkedin_cpc = by_name["LinkedIn Jobs"]["performance"]["avg_cpc"]
    assert indeed_cpc == _REGISTRY_INDEED, (
        f"seed_supply_repository.json Indeed avg_cpc {indeed_cpc} drifted "
        f"from registry {_REGISTRY_INDEED} -- this file seeds the Supabase "
        "supply_repository table (scripts/seed_supabase.py)"
    )
    assert linkedin_cpc == _REGISTRY_LINKEDIN, (
        f"seed_supply_repository.json LinkedIn Jobs avg_cpc {linkedin_cpc} "
        f"drifted from registry {_REGISTRY_LINKEDIN} -- this file seeds the "
        "Supabase supply_repository table (scripts/seed_supabase.py)"
    )


def test_publisher_benchmarks_json_linkedin_pin():
    data = json.loads(
        (PROJECT_ROOT / "data" / "publisher_benchmarks_2026.json").read_text()
    )
    linkedin = next(p for p in data["platforms"] if p["name"] == "LinkedIn")
    assert linkedin["avg_cpc_usd"] == _REGISTRY_LINKEDIN, (
        f"publisher_benchmarks_2026.json LinkedIn avg_cpc_usd "
        f"{linkedin['avg_cpc_usd']} drifted from registry "
        f"{_REGISTRY_LINKEDIN} -- this file feeds Nova RAG retrieval"
    )


if __name__ == "__main__":
    import inspect

    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            params = inspect.signature(fn).parameters
            if params:
                continue  # monkeypatch-based tests need pytest
            try:
                fn()
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    if failures:
        sys.exit(1)
    print(
        "all standalone cpc drift pins passed (run under pytest for the behavioral pins)"
    )
