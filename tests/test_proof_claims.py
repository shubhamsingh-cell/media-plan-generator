"""Every public proof number on the hub and in Nova's greeting traces to a
real artifact (proof_claims.py), and no stale hardcoded figure survives.

Pre-fix the hub said "10,341+ supply partners" (no data file states it; the
supply repository holds 7,053), "25 AI models" next to a greeting that said
"23 AI models", a footer that said "22+ Live APIs / 108,871 Jobs analyzed /
76 Countries" under a stats row that disagreed with it, "under 30 seconds"
against a measured p50 of ~48 s, and a "Trusted by" strip of companies with
no generated plan.

Three guards, in increasing strictness:
  * the denylist (``STALE``) -- figures someone already remembered;
  * the allowlist (``test_every_proof_slot_is_a_claim_token``) -- every proof
    slot in the template either carries a ``{{claim_*}}`` token or no digit at
    all, so a NEW hardcoded number cannot appear;
  * the reviewed snapshot (``tests/baselines/proof_claims_snapshot.json``) --
    the environment-independent counts are pinned, so a registry or data-file
    change that moves a public number fails CI until a person reviews it:
    ``python3 -m tests.test_proof_claims --update``.
"""

from __future__ import annotations

import gzip
import html as html_mod
import http.client
import json
import re
import sys
from pathlib import Path

import pytest

import kb_loader
import llm_router
import proof_claims
from tests.live_server import live_port  # noqa: F401 (fixture)

ROOT = Path(__file__).resolve().parent.parent
HUB = ROOT / "templates" / "hub.html"
SNAPSHOT_PATH = ROOT / "tests" / "baselines" / "proof_claims_snapshot.json"

COUNTED = (
    "supply_partners",
    "ai_models",
    "live_apis",
    "knowledge_bases",
    "countries",
    "ai_tools",
    "llm_providers",
    "jobs_analyzed",
)

# Environment-independent counts, pinned so drift is reviewed, not silent.
# ai_models (env-var model overrides) and live_apis (configured credentials)
# legitimately differ per environment and are not pinned.
PINNED = (
    "supply_partners",
    "knowledge_bases",
    "countries",
    "ai_tools",
    "llm_providers",
    "jobs_analyzed",
    # The hero card's default scenario (and story 4's figure): the engine's
    # own answer, pinned so a recalibration changes the public number on
    # purpose, never silently.
    "pe_cph",
    "pe_hires",
)

# Figures that used to be hardcoded on these surfaces and must not come back.
STALE = (
    "10,341",
    "10341",
    "10,238",
    "30 seconds",
    "under 30",
    "Plan in 30",
    "23 AI models",
    "22 live APIs",
    "22+",
    "44 knowledge bases",
    "11M+ job benchmarks",
    "23 LLM",
    "60 AI tools",
    "90+ job boards",
    "91+ job board",
    "Database tables",
    "Powering recruitment intelligence at",
    "Trusted by enterprise teams",
    "Live &middot; drag the budget",
    "Recomputes live",
    # The hero card's hand-tuned model and the figure story 4 pinned to it.
    "Illustrative model",
    "illustrative example",
    "1,064",
    # The per-industry cost-per-hire quartiles have no reachable source
    # (ledger/2026-10-17/R_cph_sources.md): never a "median" line, never NAHS.
    "Nursing median",
    "NAHS",
    "SHRM 2024",
    # SHRM's national average is an all-in cost per hire; the card's figure is
    # media spend only -- never shown side by side (trust lens, 2026-10-17).
    "SHRM 2025 US average",
    # The headline is budget / whole hires at the floor, which equals half the
    # midpoint only when the division is exact (trust lens, round 3).
    "Half the engine",
    "half its industry midpoint",
)


def _get(port: int, path: str) -> tuple[int, str]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    conn.request("GET", path, headers={"Accept": "text/html"})
    resp = conn.getresponse()
    body = resp.read()
    if resp.getheader("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    conn.close()
    return resp.status, body.decode("utf-8")


def _current_snapshot() -> dict[str, int | None]:
    c = proof_claims.claims()
    return {k: c[k].value for k in PINNED}


# ── The claims themselves ────────────────────────────────────────────────────


@pytest.mark.parametrize("key", COUNTED)
def test_counted_claim_is_a_positive_int_with_a_source(key: str) -> None:
    claim = proof_claims.claims()[key]
    assert isinstance(claim.value, int) and claim.value > 0
    assert claim.source and ("data/" in claim.source or "." in claim.source)
    assert claim.text == f"{claim.value:,}"  # no "+" -- these are exact counts


def test_supply_partners_is_the_repository_total() -> None:
    with open(
        ROOT / "data" / "joveo_global_supply_repository.json", encoding="utf-8"
    ) as f:
        repo = json.load(f)
    claim = proof_claims.claims()["supply_partners"]
    assert claim.value == repo["active_publishers"]  # the hero says "Nova scans ..."
    assert claim.value < repo["total_publishers"] and "active" in claim.source


def test_ai_models_is_the_distinct_provider_model_ids() -> None:
    ids = {c["model"] for c in llm_router.PROVIDER_CONFIG.values() if c.get("model")}
    models = {proof_claims._canonical_model(i) for i in ids}
    assert proof_claims.claims()["ai_models"].value == len(models)
    # the same model on several hosts is one model
    assert (
        proof_claims._canonical_model("@cf/meta/llama-3.3-70b-instruct-fp8-fast")
        == "llama-3.3-70b"
    )
    assert (
        proof_claims._canonical_model("Meta-Llama-3.3-70B-Instruct") == "llama-3.3-70b"
    )
    assert proof_claims._canonical_model("openai/gpt-oss-120b:free") == "gpt-oss-120b"
    assert len(models) < len(ids)
    from urllib.parse import urlparse

    hosts = {
        urlparse(c["endpoint"]).netloc.lower()
        for c in llm_router.PROVIDER_CONFIG.values()
    }
    assert proof_claims.claims()["llm_providers"].value == len(hosts)
    assert len(hosts) < len(llm_router.PROVIDER_CONFIG)  # entries are not providers


def test_knowledge_bases_is_the_kb_files_present_on_disk() -> None:
    present = {f for f in kb_loader.KB_FILES.values() if (ROOT / "data" / f).is_file()}
    assert proof_claims.claims()["knowledge_bases"].value == len(present)


def test_live_apis_counts_configured_plus_keyless_sources() -> None:
    import api_integrations
    import recruitment_apis

    registry = api_integrations.get_api_status()
    configured = [k for k, v in registry.items() if v.get("configured")]
    expected = len(configured) + len(recruitment_apis.LIVE_SOURCES)
    claim = proof_claims.claims()["live_apis"]
    assert claim.value == expected
    assert "configured" in claim.source  # the number is explicitly per-environment
    # The stubs that never make a live call must not be counted.
    keys = {k for k, _ in recruitment_apis.LIVE_SOURCES}
    assert not keys & {"warntracker", "levels", "crunchbase", "pdl"}


def test_jobs_analyzed_is_the_slotops_dataset_total() -> None:
    with open(ROOT / "data" / "slotops_benchmarks_summary.json", encoding="utf-8") as f:
        total = json.load(f)["metadata"]["total_jobs"]
    assert proof_claims.claims()["jobs_analyzed"].value == total


def test_plan_time_is_derived_from_the_committed_measurement() -> None:
    with open(ROOT / "data" / "plan_generation_timing.json", encoding="utf-8") as f:
        m = json.load(f)
    c = proof_claims.claims()
    assert 40 <= m["p50_seconds"] <= 90 and c["plan_time"].text == "about a minute"
    assert c["plan_time_short"].text == "~1\u00a0min"  # non-breaking: it sits in a tile
    assert f"n={m['n']}" in c["plan_time"].source and "p50" in c["plan_time"].source
    assert proof_claims._plan_time_text(25.0)[0] == "under a minute"
    assert proof_claims._plan_time_text(130.0) == ("about 2 minutes", "~2\u00a0min")


def test_ai_tools_excludes_the_stub_tools() -> None:
    import nova

    names = {t["name"] for t in nova.Nova.get_tool_definitions(None)}
    stubs = {n for n in names if any(k in n for k in proof_claims._STUB_TOOL_MARKERS)}
    assert stubs, "expected the four placeholder tools to be registered"
    assert proof_claims.claims()["ai_tools"].value == len(names) - len(stubs)


def test_documented_claims_carry_their_provenance() -> None:
    c = proof_claims.claims()
    assert c["soc2"].text == "Built at Joveo (SOC 2 Type II)"
    assert "joveo.com/security-and-compliance" in c["soc2"].source


def test_pinned_claims_match_the_reviewed_snapshot() -> None:
    assert (
        SNAPSHOT_PATH.is_file()
    ), "no reviewed snapshot; create it with: python3 -m tests.test_proof_claims --update"
    expected = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    current = _current_snapshot()
    assert current == expected, (
        "a public proof number drifted from the reviewed snapshot: "
        f"{ {k: (expected.get(k), v) for k, v in current.items() if expected.get(k) != v} }. "
        "Check the new value is right (it is on the hub and in Nova's greeting), then "
        "refresh with: python3 -m tests.test_proof_claims --update"
    )


def test_unreadable_source_is_omitted_not_guessed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom() -> proof_claims.ProofClaim:
        raise OSError("simulated unreadable source")

    monkeypatch.setitem(proof_claims._SOURCES, "supply_partners", boom)
    computed = proof_claims.compute()
    assert "supply_partners" not in computed
    assert "ai_models" in computed  # the others are unaffected


# ── Rendering ────────────────────────────────────────────────────────────────

_FAKE = {
    "supply_partners": proof_claims.ProofClaim(
        "supply_partners", 7053, "7,053", "test"
    ),
    "countries": proof_claims.ProofClaim("countries", 46, "46", "test"),
}


def test_render_substitutes_tokens_blocks_and_raw() -> None:
    text = (
        '{{#claim_supply_partners}}<b data-count="{{claim_supply_partners:raw}}">'
        "{{claim_supply_partners}} partners</b>{{/claim_supply_partners}}"
    )
    assert proof_claims.render(text, _FAKE) == '<b data-count="7053">7,053 partners</b>'


def test_render_drops_missing_blocks_and_keeps_inverted_fallback() -> None:
    text = (
        "{{#claim_ai_models}}{{claim_ai_models}} models{{/claim_ai_models}}"
        "{{^claim_ai_models}}many models{{/claim_ai_models}}"
        "{{#claim_supply_partners+ai_models}}both{{/claim_supply_partners+ai_models}}"
        "{{#claim_supply_partners+countries}} and both present{{/claim_supply_partners+countries}}"
    )
    assert proof_claims.render(text, _FAKE) == "many models and both present"


def test_unguarded_missing_token_degrades_to_a_quantifier_not_a_blank() -> None:
    out = proof_claims.render("across {{claim_ai_models}} AI models", _FAKE)
    assert out == "across multiple AI models"
    assert "  " not in out


def test_render_bytes_leaves_token_free_content_untouched() -> None:
    assert proof_claims.render_bytes(b"<p>no tokens</p>") == b"<p>no tokens</p>"


# ── The surfaces ─────────────────────────────────────────────────────────────

_PROOF_SLOT_PATTERNS = (
    r'<div class="products-stat-number"[^>]*>(.*?)</div>',
    r'<span class="footer-stat-value">(.*?)</span>',
    r'<div class="outcome-stat-value[^"]*">(.*?)</div>',
    r'<p class="trust-bar-label">(.*?)</p>|<div class="trust-bar-label">(.*?)</div>',
    r'<p class="channel-logos-label">(.*?)</p>',
    r'(?:og:description|twitter:description|name="description)"\s*content="(.*?)"',
    r'<p class="hero-subline[^"]*">(.*?)</p>',
    r'<div class="moment-3 ">.*?<div class="moment-text">.*?<p>(.*?)</p>|class="story-moment moment-3".*?<div class="moment-text">.*?<p>(.*?)</p>',
    r'class="product-hero-card product-plan".*?<p>(.*?)</p>',
    r'desc: "(.*?)",',
)
_TOKEN_OR_BLOCK = re.compile(r"\{\{[#^/]?claim_[a-z0-9_+]+(?::raw)?\}\}")


def _digits_outside_tokens(text: str) -> bool:
    """True when a slot still has a digit once every claim token is stripped."""
    stripped = _TOKEN_OR_BLOCK.sub("", text)
    stripped = re.sub(r"<svg.*?</svg>|<[^>]+>", "", stripped, flags=re.S)
    stripped = stripped.replace("GeoViz 3D", "")  # a product name, not a figure
    return re.search(r"\d", stripped) is not None


def test_every_proof_slot_is_a_claim_token() -> None:
    """Once the {{claim_*}} tokens are stripped, no proof slot may contain a
    digit -- never a hardcoded number next to a computed one. This is the
    allowlist the denylist cannot be."""
    src = HUB.read_text(encoding="utf-8")
    offenders = []
    for pattern in _PROOF_SLOT_PATTERNS:
        for m in re.finditer(pattern, src, re.S):
            text = next((g for g in m.groups() if g is not None), "")
            if _digits_outside_tokens(text):
                offenders.append(text.strip()[:80])
    trust = re.search(r'<ul class="hero-trust".*?</ul>', src, re.S)
    assert trust, "hero trust list missing"
    for li in re.findall(r"<li>(.*?)</li>", trust.group(0), re.S):
        if _digits_outside_tokens(li):
            offenders.append(li.strip()[:80])
    assert not offenders, f"hardcoded figures in proof slots: {offenders}"


def test_hub_template_carries_tokens_not_numbers() -> None:
    src = HUB.read_text(encoding="utf-8")
    assert "{{claim_supply_partners}}" in src
    assert "{{claim_supply_partners:raw}}" in src
    assert "{{claim_soc2}}" in src
    for stale in STALE:
        assert stale not in src, f"stale figure {stale!r} still in hub.html"


def test_trust_bar_names_have_an_evidence_row() -> None:
    """Every marquee name has a row in docs/TRUST_BAR_PROVENANCE.md, and the
    label is the approved wording (b029de8 silently reverted both in July)."""
    src = HUB.read_text(encoding="utf-8")
    ledger = (ROOT / "docs" / "TRUST_BAR_PROVENANCE.md").read_text(encoding="utf-8")
    evidenced = {
        row.split("|")[1].strip()
        for row in ledger.split("## Removed")[0].splitlines()
        if row.startswith("| ") and "---" not in row and not row.startswith("| Name")
    }
    names = {
        html_mod.unescape(n.strip())
        for n in re.findall(
            r'<span class="marquee-item[^"]*"[^>]*>\s*([^<]*?)\s*</span', src
        )
    }
    assert names, "trust-bar marquee is empty"
    assert (
        names <= evidenced
    ), f"marquee names without an evidence row: {sorted(names - evidenced)}"
    assert "Built on the Joveo platform powering recruitment at" in src
    assert 'aria-label="Enterprises recruiting on the Joveo platform"' in src


def test_hub_is_served_with_the_computed_numbers(live_port: int) -> None:
    status, html = _get(live_port, "/")
    assert status == 200
    c = proof_claims.claims()
    supply = c["supply_partners"]
    assert f"{supply.text} supply partners" in html
    assert f'data-count="{supply.value}"' in html
    for key in ("ai_models", "live_apis", "knowledge_bases", "countries"):
        assert f'data-count="{c[key].value}"' in html, key
    # the stats row and the footer are the same numbers, rendered from one dict
    assert f'<span class="footer-stat-value">{c["live_apis"].text}</span>' in html
    assert f'<span class="footer-stat-value">{c["jobs_analyzed"].text}</span>' in html
    assert f'<span class="footer-stat-value">{c["countries"].text}</span>' in html
    assert "about a minute" in html
    assert "Built at Joveo (SOC 2 Type II)" in html
    # The hero card and story 4 carry the engine's default-scenario figure
    # (tests/test_hub_prediction_engine.py checks it against the engine).
    assert "Nova engine &middot; drag the budget" in html
    assert f'data-target="{c["pe_cph"].value}"' in html
    assert (
        f'<span class="moment-figure-scope">{c["pe_scenario_html"].text}</span>' in html
    )
    assert (
        "{{claim_" not in html and "{{/claim_" not in html and "{{#claim_" not in html
    )
    for stale in STALE:
        assert stale not in html, f"stale figure {stale!r} served on the hub"


def test_proof_claims_endpoint_lists_values_with_sources(live_port: int) -> None:
    status, body = _get(live_port, "/api/proof-claims")
    assert status == 200
    data = json.loads(body)
    for key in COUNTED:
        entry = data["claims"][key]
        assert entry["value"] > 0 and entry["source"]
    assert data["omitted"] == []


def test_deploy_ready_reports_proof_claim_omissions(live_port: int) -> None:
    _, body = _get(live_port, "/api/deploy/ready")
    data = json.loads(body)
    assert data["proof_claims"]["omitted"] == []
    assert (
        data["proof_claims"]["values"]["supply_partners"]
        == proof_claims.claims()["supply_partners"].text
    )


def test_nova_greeting_uses_the_same_numbers() -> None:
    import nova

    c = proof_claims.claims()
    greeting = nova.nova_default_greeting()
    for key, label in (
        ("supply_partners", "supply partners"),
        ("ai_models", "AI models"),
        ("live_apis", "live APIs"),
        ("knowledge_bases", "knowledge bases"),
        ("countries", "countries"),
    ):
        assert f"**{c[key].text} {label}**" in greeting, key
    for stale in STALE:
        assert stale not in greeting
    assert c["supply_partners"].text in proof_claims.reach_phrase()


def test_no_stale_figures_in_sources() -> None:
    for rel in ("nova.py", "static/js/hub.js", "static/css/hub.css"):
        src = (ROOT / rel).read_text(encoding="utf-8")
        for stale in (
            "10,341",
            "10341",
            "10,238",
            "23 AI models",
            "22 live APIs",
            "44 knowledge bases",
            "11M+ job benchmarks",
            "70+ countries",
        ):
            assert stale not in src, f"{rel} still contains {stale!r}"


def test_learned_answer_tokens_are_rendered() -> None:
    import nova

    tokened = [a for a in nova._PRELOADED_ANSWERS if "{{claim_" in a["answer"]]
    assert tokened, "expected the Joveo-reach FAQ answers to carry claim tokens"
    for entry in tokened:
        rendered = proof_claims.render(entry["answer"])
        assert "{{" not in rendered and "}}" not in rendered
    assert re.search(
        r"\*\*[\d,]+ Supply Partners\*\*", proof_claims.render(tokened[0]["answer"])
    )


if __name__ == "__main__":
    if "--update" in sys.argv:
        SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT_PATH.write_text(
            json.dumps(_current_snapshot(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"snapshot written: {SNAPSHOT_PATH} {_current_snapshot()}")
    else:
        sys.exit(pytest.main([__file__, "-q"]))
