"""No raw snake_case internal key may reach client text.

Prod (AWP Safety, 2026-09-25) and the 2026-10-01 B_sweep shipped critical
``snake_case_leak`` findings for: industry keys in the entity-validation
fallback "<Client> is a company in the <industry_key> industry." (slide 8 +
Market Intelligence -- construction_real_estate, tech_engineering,
retail_consumer, general_entry_level), the benchmark data-set id
``international_benchmarks_2026`` on every non-US plan's Intl Benchmarks
sheet, and the gold_standard supply tier ``critically_scarce`` in the
slide-8 geography rationale.

Two layers of proof:
  1. Exhaustive + cheap: every industry key the app can produce
     (INDUSTRY_LABEL_MAP, INDUSTRY_ALLOC_PROFILES, classify_industry's
     legacy keys, the wizard's data-industry values) through the shared
     humanizer and the synthesizer's company-profile path; every
     gold_standard supply tier through the geography rationale.
  2. Headless generation over a representative industry matrix (incl. a
     non-US plan): every slide text / table cell / chart XML text node /
     workbook cell / sheet name / chart label scanned for the token shape
     bundle_qa flags.
"""

from __future__ import annotations

import io
import re
import sys
import zipfile
from pathlib import Path
from xml.sax.saxutils import unescape

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import bundle_qa  # noqa: E402
import data_synthesizer  # noqa: E402
import display_format  # noqa: E402
import insight_composer  # noqa: E402

TOKEN_RE = re.compile(r"\b[a-z0-9]+(?:_[a-z0-9]+)+\b")  # bundle_qa's shape
URL_RE = re.compile(r"https?://|www\.|[\w.+-]+@[\w-]+\.[\w.-]+")
# Documented allowlist: lowercase-underscore strings that are legitimate in
# client text. Empty today -- every hit is a leak. URLs/e-mails are exempt
# by URL_RE (same exemption bundle_qa applies).
ALLOWLIST: frozenset = frozenset()

RIFLE = (
    "The Accuracy International Arctic Warfare rifle is a bolt-action sniper "
    "rifle designed and manufactured by the British company Accuracy "
    "International."
)


def _leaks(text: str) -> list:
    if not isinstance(text, str) or URL_RE.search(text):
        return []
    return [t for t in TOKEN_RE.findall(text) if t not in ALLOWLIST]


def _all_industry_keys() -> list:
    import app
    from ppt_generator import INDUSTRY_ALLOC_PROFILES
    from shared_utils import INDUSTRY_LABEL_MAP

    keys = set(INDUSTRY_LABEL_MAP) | set(INDUSTRY_ALLOC_PROFILES)
    keys |= {v.get("legacy_key") for v in app.INDUSTRY_NAICS_MAP.values() if v.get("legacy_key")}
    wizard = (PROJECT_ROOT / "templates" / "partials" / "index" / "body_content.html").read_text()
    keys |= set(re.findall(r'data-industry="([a-z_]+)"', wizard))
    return sorted(k for k in keys if k)


def test_industry_key_universe_is_nonempty_and_includes_prod_leaks():
    keys = _all_industry_keys()
    for k in ("construction_real_estate", "tech_engineering", "general_entry_level", "rideshare"):
        assert k in keys
    assert len(keys) >= 23


@pytest.mark.parametrize("key", _all_industry_keys())
def test_every_industry_key_humanizes(key):
    for prose in (False, True):
        label = display_format.humanize_key(key, prose=prose)
        assert label and "_" not in label, (key, label)
        assert not _leaks(label)


@pytest.mark.parametrize("key", _all_industry_keys())
def test_failed_entity_profile_never_renders_the_industry_key(key):
    """The prod leak site: entity validation fails -> the profile used to
    get '<Client> is a company in the <key> industry.'"""
    res = data_synthesizer.fuse_competitive_intelligence(
        {"company_info": {"description": RIFLE}, "company_metadata": {"extract": RIFLE}},
        {},
        {"company_name": "Northwind Partners", "industry": key},
    )
    prof = res["company_profile"]
    for field in ("description", "summary"):
        assert prof.get(field) == "", (key, prof.get(field))
    assert (res.get("company_wikipedia") or {}).get("description") == ""
    visible = [v for k, v in prof.items() if isinstance(v, str) and not k.startswith("_")]
    assert not any(_leaks(v) for v in visible), visible


def test_every_supply_tier_reads_as_words_with_the_right_article():
    import gold_standard

    for _threshold, tier in gold_standard._SUPPLY_TIERS:
        s = insight_composer.geography_rationale("Seattle, WA", {"supply_tier": tier})
        assert not _leaks(s), s
        assert " a abundant" not in s and " a a" not in s
    s = insight_composer.geography_rationale("Seattle, WA", {"supply_tier": "critically_scarce"})
    assert "critically scarce talent-supply tier" in s


def test_snake_tokens_in_free_text_humanize():
    assert (
        display_format.humanize_snake_tokens(
            "AWP Safety is a company in the construction_real_estate industry."
        )
        == "AWP Safety is a company in the Construction & Real Estate industry."
    )
    assert display_format.humanize_key("international_benchmarks_2026") == (
        "International Benchmarks 2026"
    )
    # URLs keep their underscores
    url = "see https://example.com/a_b_c"
    assert display_format.humanize_snake_tokens(url) == url


# ---------------------------------------------------------------------------
# Headless generation matrix
# ---------------------------------------------------------------------------
MATRIX = [
    ("construction_real_estate", ["Wheeling, WV", "Nashville, TN"], ["Traffic Control Flagger"]),
    ("tech_engineering", ["Seattle, WA", "Austin, TX"], ["Software Engineer"]),
    ("general_entry_level", ["Columbus, OH"], ["Warehouse Associate"]),
    ("retail_consumer", ["Chicago, IL", "Dallas, TX"], ["Store Associate"]),
    ("healthcare_medical", ["Phoenix, AZ"], ["Registered Nurse"]),
    ("finance_banking", ["London, UK", "Manchester, UK"], ["Financial Analyst"]),
    ("rideshare", ["Denver, CO"], ["Delivery Driver"]),
]


def _scan_pptx(blob: bytes) -> list:
    out = []
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for name in zf.namelist():
            if not name.endswith(".xml") or not name.startswith(
                ("ppt/slides/slide", "ppt/notesSlides/", "ppt/charts/")
            ):
                continue
            xml = zf.read(name).decode("utf-8", "replace")
            for m in re.finditer(r"<(?:a:t|c:v)(?:\s[^>]*)?>([^<]*)</(?:a:t|c:v)>", xml):
                txt = unescape(m.group(1))
                out += [(name, t, txt[:120]) for t in _leaks(txt)]
    return out


def _scan_xlsx(blob: bytes) -> list:
    import openpyxl

    out = []
    wb = openpyxl.load_workbook(io.BytesIO(blob))
    for ws in wb.worksheets:
        out += [(f"sheet:{ws.title}", t, ws.title) for t in _leaks(ws.title)]
        for row in ws.iter_rows():
            for c in row:
                if isinstance(c.value, str) and not c.value.startswith("="):
                    out += [(f"{ws.title}!{c.coordinate}", t, c.value[:120]) for t in _leaks(c.value)]
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for name in zf.namelist():
            if name.startswith("xl/charts/") and name.endswith(".xml"):
                xml = zf.read(name).decode("utf-8", "replace")
                for m in re.finditer(r"<(?:a:t|c:v)(?:\s[^>]*)?>([^<]*)</(?:a:t|c:v)>", xml):
                    out += [(name, t, m.group(1)[:120]) for t in _leaks(unescape(m.group(1)))]
    return out


@pytest.mark.parametrize("industry,locations,roles", MATRIX)
def test_generated_bundle_has_no_snake_case_anywhere(industry, locations, roles):
    import excel_v2
    import ppt_generator
    from tools_regen_bundles import build_plan_data

    client = f"Northwind {industry.split('_')[0].title()} Partners"
    data = build_plan_data(
        {
            "client_name": client,
            "industry": industry,
            "budget": "$240,000",
            "campaign_duration": "6 months",
            "hire_volume": "50-100 hires",
            "locations": locations,
            "roles": roles,
            "target_roles": roles,
            "competitors": ["Acme Staffing"],
        }
    )
    enriched = {"company_info": {"description": RIFLE}}
    data["_enriched"] = enriched
    data["_synthesized"] = {
        "competitive_intelligence": data_synthesizer.fuse_competitive_intelligence(
            enriched, {}, {"company_name": client, "industry": data["industry"]}
        )
    }
    if not locations[0].endswith(", US") and locations[0].split(", ")[-1] == "UK":
        # Same injection app.py's /api/generate makes for a non-US region
        # (the Intl Benchmarks sheet + its data-set "source" id).
        from kb_loader import load_knowledge_base

        kb_intl = load_knowledge_base().get("international_benchmarks", {})
        regions = kb_intl.get("regions", {})
        countries = kb_intl.get("countries", {})
        data["target_region"] = "emea"
        data["_intl_benchmarks"] = {
            "countries": {
                c: countries[c]
                for c in regions.get("emea", {}).get("countries", [])
                if c in countries
            },
            "regions": {k: v for k, v in regions.items() if k == "emea"},
            "source": "international_benchmarks_2026 (38 countries)",
        }
        assert data["_intl_benchmarks"]["countries"], "KB intl benchmarks missing"
    # A city record carrying the raw tier key reaches the slide-8 rationale.
    gs = data.setdefault("_gold_standard", {})
    cld = gs.setdefault("city_level_data", {})
    cld[locations[0].split(",")[0].strip()] = {"supply_tier": "critically_scarce"}

    pptx_bytes = ppt_generator.generate_pptx(dict(data))
    xlsx_bytes = excel_v2.generate_excel_v2(dict(data))
    leaks = _scan_pptx(pptx_bytes) + _scan_xlsx(xlsx_bytes)
    assert not leaks, leaks[:10]
    findings = bundle_qa.run_bundle_qa(pptx_bytes, xlsx_bytes, data)
    assert not [f for f in findings if f.get("code") == "snake_case_leak"], findings
