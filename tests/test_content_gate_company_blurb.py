"""Wrong-entity company blurbs must never reach a deliverable.

Prod defect (AWP Safety, 2026-09-25, Render log 12:58:23Z): the Wikipedia
name-guess lookup (api_enrichment.fetch_company_info) accepted the article
for the AWP *sniper rifle* -- its extract contains the word "company"
("...manufactured by the British company Accuracy International") -- and
slide 1 printed "...sniper rifle designed and manufactured by the
British..." under the client's name. data_synthesizer's own entity check
logged "Entity validation FAILED" but only swapped a
"<Client> is a company in the construction_real_estate industry." fallback
into slide 8 / Market Intelligence (a snake_case leak), while the cover read
the raw lookup and the workbook printed " Entity Mismatch Reason: ...rifle"
diagnostics rows.

Fixtures below are recorded-shape Wikipedia REST summary responses (type /
title / extract, first-sentence shape of the real article; text is
paraphrased test data). Everything is faked at the HTTP boundary
(api_enrichment._http_get_json); no network call is made.
"""

from __future__ import annotations

import io
import random
import re
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import company_blurb  # noqa: E402
import data_synthesizer  # noqa: E402

RIFLE = {
    "type": "standard",
    "title": "Accuracy International Arctic Warfare",
    "extract": (
        "The Accuracy International Arctic Warfare rifle is a bolt-action "
        "sniper rifle designed and manufactured by the British company "
        "Accuracy International. It has been in production since the 1980s."
    ),
}
HERSHEY = {
    "type": "standard",
    "title": "The Hershey Company",
    "extract": (
        "The Hershey Company, commonly known as Hershey's, is an American "
        "multinational company and one of the largest chocolate manufacturers "
        "in the world. It also manufactures baked products."
    ),
}

# (client name, summary fixture, expected accept)
CASES = [
    ("AWP Safety", RIFLE, False),
    ("AWP", RIFLE, False),
    ("Apple", {"type": "standard", "title": "Apple Inc.", "extract": "Apple Inc. is an American multinational corporation and technology company headquartered in Cupertino, California."}, True),
    ("Apple", {"type": "standard", "title": "Apple", "extract": "An apple is a round, edible fruit produced by an apple tree."}, False),
    ("Amazon", {"type": "standard", "title": "Amazon (company)", "extract": "Amazon.com, Inc., doing business as Amazon, is an American multinational technology company engaged in e-commerce and cloud computing."}, True),
    ("Amazon", {"type": "standard", "title": "Amazon River", "extract": "The Amazon River in South America is the largest river by discharge volume of water in the world."}, False),
    ("Delta", {"type": "standard", "title": "Delta Air Lines", "extract": "Delta Air Lines, Inc. is a major airline of the United States headquartered in Atlanta, Georgia."}, True),
    ("Delta", {"type": "standard", "title": "River delta", "extract": "A river delta is a landform shaped like a triangle, created by the deposition of sediment."}, False),
    ("Shell", {"type": "standard", "title": "Shell plc", "extract": "Shell plc is a British multinational oil and gas company headquartered in London, England."}, True),
    ("Shell", {"type": "standard", "title": "Seashell", "extract": "A seashell or sea shell, also known simply as a shell, is a hard, protective outer layer."}, False),
    ("Target", {"type": "standard", "title": "Target Corporation", "extract": "Target Corporation is an American retail corporation that operates a chain of discount department stores."}, True),
    ("Target", {"type": "standard", "title": "Target", "extract": "A target is an object or area that is aimed at."}, False),
    ("Gap", {"type": "standard", "title": "Gap Inc.", "extract": "The Gap, Inc., commonly known as Gap, is an American worldwide clothing and accessories retailer."}, True),
    ("Gap", {"type": "standard", "title": "Cumberland Gap", "extract": "Cumberland Gap is a pass through the long ridge of the Cumberland Mountains."}, False),
    ("Visa", {"type": "standard", "title": "Visa Inc.", "extract": "Visa Inc. is an American multinational payment card services corporation headquartered in San Francisco."}, True),
    ("Visa", {"type": "standard", "title": "Travel visa", "extract": "A travel visa is a conditional authorization granted by a polity to a foreigner."}, False),
    ("Ford", {"type": "standard", "title": "Ford Motor Company", "extract": "Ford Motor Company (commonly known as Ford) is an American multinational automobile manufacturer headquartered in Dearborn, Michigan."}, True),
    ("Ford", {"type": "standard", "title": "Harrison Ford", "extract": "Harrison Ford (born July 13, 1942) is an American actor."}, False),
    ("Ford", {"type": "standard", "title": "Henry Ford", "extract": "Henry Ford was an American industrialist and business magnate."}, False),
    ("Mercury", {"type": "standard", "title": "Mercury (planet)", "extract": "Mercury is the first planet from the Sun and the smallest in the Solar System."}, False),
    ("Mercury", {"type": "standard", "title": "Mercury (element)", "extract": "Mercury is a chemical element; it has symbol Hg and atomic number 80."}, False),
    ("Mercury", {"type": "standard", "title": "Freddie Mercury", "extract": "Freddie Mercury was a British singer and songwriter."}, False),
    ("THE HERSHEY COMPANY", HERSHEY, True),
    ("McDonald's", {"type": "standard", "title": "McDonald's", "extract": "McDonald's Corporation is an American multinational fast food chain, founded in 1940 as a restaurant."}, True),
    ("ADP", {"type": "standard", "title": "ADP (company)", "extract": "Automatic Data Processing, Inc. (ADP) is an American provider of human resources management software and services."}, True),
    # B_sweep (2026-10-01) wrong-entity covers -- e, k, o were logged
    # "Entity validation FAILED" yet shipped; i and m passed the old check
    # (a client token appeared somewhere in the first sentence).
    ("Hoshino Electronics Co., Ltd.", {"type": "standard", "title": "Ibanez", "extract": "Ibanez is a Japanese guitar brand owned by Hoshino Gakki. Based in Nagoya, Aichi, Japan."}, False),
    ("Brightway Home Stores", {"type": "standard", "title": "Economic impact of the COVID-19 pandemic in Malaysia", "extract": "The COVID-19 pandemic in Malaysia has had a significant impact on the Malaysian economy, leading to the devaluation of the ringgit."}, False),
    ("Riverbend Community Food Bank", {"type": "standard", "title": "Duke Energy", "extract": "Duke Energy Corporation is an American electric power and natural gas holding company headquartered in Charlotte, North Carolina."}, False),
    ("Orbit Cloud Technologies", {"type": "standard", "title": "Google", "extract": "Google LLC is an American multinational technology corporation focused on information technology, online advertising, search engine technology, cloud computing and artificial intelligence."}, False),
    ("Kestrel Analytics", {"type": "standard", "title": "Anysphere", "extract": "Anysphere, Inc. is an American applied research company that develops the Cursor code editor."}, False),
    ("JPMorgan Chase & Co.", {"type": "standard", "title": "JPMorgan Chase", "extract": "JPMorgan Chase & Co. is an American multinational banking institution headquartered in New York City."}, True),
    ("3M Company", {"type": "standard", "title": "3M", "extract": "The 3M Company is an American multinational conglomerate operating in the fields of industry, worker safety, and consumer goods."}, True),
    ("AT&T", {"type": "standard", "title": "AT&T", "extract": "AT&T Inc., an abbreviation of its predecessor's original name, the American Telephone and Telegraph Company, is an American multinational telecommunications holding company."}, True),
    ("Delta", {"type": "disambiguation", "title": "Delta", "extract": "Delta most commonly refers to the fourth letter of the Greek alphabet. Delta may also refer to:"}, False),
    ("Acme Widgets", {"type": "standard", "title": "Acme Widgets", "extract": ""}, False),
]


@pytest.mark.parametrize("client,summary,expected", CASES)
def test_entity_validator_recorded_shapes(client, summary, expected):
    ok, why = company_blurb.validate_company_description(
        client,
        summary["extract"],
        title=summary["title"],
        page_type=summary["type"],
    )
    assert ok is expected, f"{client!r} vs {summary['title']!r}: {why}"


# ---------------------------------------------------------------------------
# api_enrichment.fetch_company_info -- faked at the HTTP boundary
# ---------------------------------------------------------------------------
class _FakeWiki:
    def __init__(self, summaries: dict, search: dict) -> None:
        self.summaries = summaries
        self.search = search
        self.urls: list[str] = []

    def __call__(self, url, headers=None, timeout=None, **_kw):
        import urllib.parse as up

        self.urls.append(url)
        p = up.urlparse(url)
        if "/page/summary/" in p.path:
            title = up.unquote(p.path.split("/page/summary/", 1)[1]).replace("_", " ")
            return self.summaries.get(title)
        if p.path.endswith("/w/api.php"):
            q = up.parse_qs(p.query).get("srsearch", [""])[0]
            return {"query": {"search": [{"title": t} for t in self.search.get(q, [])]}}
        return None


@pytest.fixture
def wiki(monkeypatch):
    cache: dict = {}
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda k: cache.get(k))
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda k, v: cache.__setitem__(k, v))
    monkeypatch.setattr(api_enrichment, "fetch_company_logo", lambda _d: None)

    def _install(summaries, search):
        fake = _FakeWiki(summaries, search)
        monkeypatch.setattr(api_enrichment, "_http_get_json", fake)
        return fake, cache

    return _install


def test_awp_rifle_article_is_never_accepted(wiki):
    """The exact prod path: no '(company)' page, the search API's top hit is
    the rifle article -- which the old 'contains a business word' test
    accepted."""
    wiki({RIFLE["title"]: RIFLE}, {"AWP Safety company": [RIFLE["title"]]})
    info = api_enrichment.fetch_company_info("AWP Safety")
    assert "description" not in info, info.get("description")


def test_correct_company_article_is_accepted_with_title(wiki):
    wiki({HERSHEY["title"]: HERSHEY}, {"THE HERSHEY COMPANY company": [HERSHEY["title"]]})
    info = api_enrichment.fetch_company_info("THE HERSHEY COMPANY")
    assert info.get("description", "").startswith("The Hershey Company")
    assert info.get("wiki_title") == "The Hershey Company"


def test_name_with_no_page_has_no_description(wiki):
    wiki({}, {})
    assert "description" not in api_enrichment.fetch_company_info("Northwind Traders")


def test_disambiguation_page_skipped_and_search_hit_needs_the_clients_title(wiki):
    """'Delta' lands on a disambiguation page (rejected); the search
    fallback's 'Delta Air Lines' is NOT the client's name, so it is not
    trusted either (verifier 2026-10-01: search hits are where same-word
    companies come from). The client typed as 'Delta Air Lines' resolves by
    direct title."""
    disamb = {"type": "disambiguation", "title": "Delta", "extract": "Delta may refer to:"}
    airline = next(s for c, s, _ in CASES if s["title"] == "Delta Air Lines")
    wiki(
        {"Delta": disamb, "Delta Air Lines": airline},
        {"Delta company": ["Delta", "Delta Air Lines"]},
    )
    assert "description" not in api_enrichment.fetch_company_info("Delta")
    info = api_enrichment.fetch_company_info("Delta Air Lines")
    assert info.get("wiki_title") == "Delta Air Lines"


# Recorded live shapes (Wikipedia REST, 2026-10-01) -- the verifier's
# wrong-company cases.
EMERITUS = {"type": "standard", "title": "Emeritus Senior Living", "extract": "Emeritus Corporation doing business as Emeritus Senior Living was a provider of independent living, assisted living, Alzheimer's care, and skilled nursing for seniors living in Emeritus communities throughout the United States."}
BROOKDALE = {"type": "standard", "title": "Brookdale Senior Living", "extract": "Brookdale Senior Living Solutions owns and operates retirement homes across the United States. The company was established in 1978 and is based in Brentwood, Tennessee."}
JNJ = {"type": "standard", "title": "Johnson & Johnson", "extract": "Johnson & Johnson (J&J) is an American multinational pharmaceutical, biotechnology, and medical technologies corporation headquartered in New Brunswick, New Jersey."}
JNJ_COMMAS = {"type": "standard", "title": "Johnson & Johnson", "extract": "Johnson & Johnson is an American multinational, pharmaceutical, and medical technologies corporation headquartered in New Brunswick, New Jersey."}
KAISER = {"type": "standard", "title": "Kaiser Permanente", "extract": "Kaiser Permanente is an American integrated managed care consortium headquartered in Oakland, California."}
SAME_NAME_OTHER_SECTOR = [
    ("AWP", "construction_real_estate", {"type": "standard", "title": "Awp Finanznachrichten", "extract": "Awp Finanznachrichten AG is a leading Swiss business news agency based in Zurich, Switzerland."}),
    ("Pinnacle", "healthcare_medical", {"type": "standard", "title": "Pinnacle Foods", "extract": "Pinnacle Foods, Inc., is a packaged foods company headquartered in Parsippany, New Jersey, that specializes in shelf-stable and frozen foods."}),
    ("Delta", "construction_real_estate", {"type": "standard", "title": "Delta (company)", "extract": "DELTA is a cable operator in the Netherlands, providing digital cable television, Internet, and telephone service to residential and commercial customers."}),
    ("Mercury", "logistics_supply_chain", {"type": "standard", "title": "Mercury (automobile)", "extract": "Mercury was a brand of medium-priced automobiles that was produced by American manufacturer Ford Motor Company between the 1939 and 2011 motor years."}),
    ("Atlas", "logistics_supply_chain", {"type": "standard", "title": "Atlas Copco", "extract": "Atlas Copco Group is a Swedish multinational industrial company. It manufactures compressors, vacuum equipment, pumps, generators and assembly tools."}),
    ("Liberty", "healthcare_medical", {"type": "standard", "title": "Liberty Mutual", "extract": "Liberty Mutual Insurance Company is an American diversified global insurer and the sixth-largest property and casualty insurer in the world."}),
]


@pytest.mark.parametrize("client,industry,summary", SAME_NAME_OTHER_SECTOR)
def test_same_name_company_in_another_sector_is_omitted(client, industry, summary):
    ok, why = company_blurb.validate_company_description(
        client, summary["extract"], title=summary["title"], industry=industry
    )
    assert not ok, why


def test_same_article_is_accepted_when_the_sector_agrees():
    pinnacle = SAME_NAME_OTHER_SECTOR[1][2]
    liberty = SAME_NAME_OTHER_SECTOR[5][2]
    assert company_blurb.validate_company_description(
        "Pinnacle Foods", pinnacle["extract"], title=pinnacle["title"], industry="food_beverage"
    )[0]
    assert company_blurb.validate_company_description(
        "Liberty Mutual", liberty["extract"], title=liberty["title"], industry="insurance"
    )[0]


def test_full_distinctive_name_required_not_shared_words():
    ok, _ = company_blurb.validate_company_description(
        "Brookdale Senior Living", EMERITUS["extract"], title=EMERITUS["title"],
        industry="healthcare_medical",
    )
    assert not ok
    # the right article -- a verb-led definition -- is accepted
    ok, why = company_blurb.validate_company_description(
        "Brookdale Senior Living", BROOKDALE["extract"], title=BROOKDALE["title"],
        industry="healthcare_medical",
    )
    assert ok, why


@pytest.mark.parametrize(
    "summary,industry",
    [(JNJ, "pharma_biotech"), (JNJ_COMMAS, "pharma_biotech"), (KAISER, "healthcare_medical")],
)
def test_definition_is_not_cut_at_the_first_comma(summary, industry):
    ok, why = company_blurb.validate_company_description(
        summary["title"], summary["extract"], title=summary["title"], industry=industry
    )
    assert ok, why


def test_search_fallback_hit_with_another_title_is_not_trusted(wiki):
    """Prod-shaped: no direct 'Brookdale Senior Living' page; the search's
    top hit is a different company sharing two words."""
    wiki(
        {"Emeritus Senior Living": EMERITUS},
        {"Brookdale Senior Living company": ["Emeritus Senior Living"]},
    )
    assert "description" not in api_enrichment.fetch_company_info("Brookdale Senior Living")


def test_cover_needs_a_plan_industry_to_show_a_blurb():
    data = {
        "client_name": "The Hershey Company",
        "_enriched": {"company_info": {"description": HERSHEY["extract"]}},
    }
    assert company_blurb.client_company_description(data) == ""
    data["industry"] = "food_beverage"
    assert company_blurb.client_company_description(data).startswith("The Hershey Company")


def test_cached_wrong_entity_is_revalidated_on_cache_hit(wiki):
    """Prod caches (L1-L4 incl. Supabase) may hold the rifle extract under
    the pre-fix key; neither it nor a v2 entry may come back unvalidated."""
    _fake, cache = wiki({}, {})
    cache[api_enrichment._cache_key("wikipedia", "AWP Safety")] = RIFLE["extract"]
    cache[api_enrichment._cache_key("wikipedia_v3", "AWP Safety")] = {
        "extract": RIFLE["extract"],
        "title": RIFLE["title"],
    }
    assert "description" not in api_enrichment.fetch_company_info("AWP Safety")


def test_clearbit_suggestion_for_another_company_is_dropped(monkeypatch):
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda k: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda k, v: None)
    monkeypatch.setattr(
        api_enrichment,
        "_http_get_json",
        lambda *a, **k: [{"name": "Accuracy International", "domain": "accuracyinternational.com"}],
    )
    assert api_enrichment.fetch_company_metadata("AWP Safety") is None
    # A client-supplied website still wins (trusted input), name kept.
    res = api_enrichment.fetch_company_metadata("AWP Safety", "https://awpsafety.com")
    assert res["domain"] == "awpsafety.com"


# ---------------------------------------------------------------------------
# Every surface that reads the lookup: synthesizer profile, cover, slide 8,
# Market Intelligence
# ---------------------------------------------------------------------------
def test_synthesized_profile_omits_failed_blurb_and_never_falls_back():
    res = data_synthesizer.fuse_competitive_intelligence(
        {"company_info": {"description": RIFLE["extract"]}},
        {},
        {"company_name": "AWP Safety", "industry": "construction_real_estate"},
    )
    prof = res["company_profile"]
    assert prof["description"] == "" and prof["summary"] == ""
    assert prof["_entity_mismatch"] is True
    blob = repr(res)
    assert "is a company in the" not in blob
    assert "construction_real_estate industry" not in blob


def _bundle_for(client: str, description: str, industry: str = "construction_real_estate"):
    import excel_v2
    import ppt_generator
    from tools_regen_bundles import build_plan_data

    brief = {
        "client_name": client,
        "industry": industry,
        "budget": "$150,000",
        "campaign_duration": "6 months",
        "hire_volume": "50-100 hires",
        "locations": ["Wheeling, WV", "Nashville, TN"],
        "roles": ["Traffic Control Flagger"],
        "target_roles": ["Traffic Control Flagger"],
        "competitors": ["Flagger Force"],
    }
    data = build_plan_data(brief)
    enriched = {"company_info": {"description": description}}
    data["_enriched"] = enriched
    data["_synthesized"] = {
        "competitive_intelligence": data_synthesizer.fuse_competitive_intelligence(
            enriched, {}, {"company_name": client, "industry": data["industry"]}
        )
    }
    return ppt_generator.generate_pptx(dict(data)), excel_v2.generate_excel_v2(dict(data))


def _deck_text(pptx_bytes: bytes) -> list[list[str]]:
    from pptx import Presentation

    slides = []
    for slide in Presentation(io.BytesIO(pptx_bytes)).slides:
        lines = []
        for sh in slide.shapes:
            if getattr(sh, "has_text_frame", False):
                lines += [p.text for p in sh.text_frame.paragraphs if p.text.strip()]
        slides.append(lines)
    return slides


def _sheet_strings(xlsx_bytes: bytes) -> list[str]:
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes))
    return [
        c.value
        for ws in wb.worksheets
        for row in ws.iter_rows()
        for c in row
        if isinstance(c.value, str)
    ]


def test_awp_bundle_has_no_rifle_blurb_anywhere():
    pptx_bytes, xlsx_bytes = _bundle_for("AWP Safety", RIFLE["extract"])
    slides = _deck_text(pptx_bytes)
    deck = "\n".join("\n".join(s) for s in slides)
    cells = "\n".join(_sheet_strings(xlsx_bytes))
    for blob in (deck, cells):
        assert "sniper" not in blob.lower() and "rifle" not in blob.lower()
        assert "is a company in the" not in blob
        assert "Entity Mismatch" not in blob
    assert "AWP Safety" in "\n".join(slides[0])  # cover still names the client


def test_correct_blurb_renders_on_cover_at_a_clean_boundary():
    pptx_bytes, _ = _bundle_for("THE HERSHEY COMPANY", HERSHEY["extract"], "food_beverage")
    cover = _deck_text(pptx_bytes)[0]
    tagline = [t for t in cover if t.startswith("The Hershey Company, commonly known")]
    assert tagline, cover
    t = tagline[0]
    assert len(t) <= 121
    # whole sentence, or a whole-word cut marked with an ellipsis
    assert t.endswith(".") or (t.endswith("…") and t[-2].isalpha())
    assert t.rstrip("…") in HERSHEY["extract"]
    nxt = HERSHEY["extract"][len(t.rstrip("…"))]
    assert not nxt.isalnum(), f"cut mid-word: {t!r}"


def test_truncation_never_cuts_mid_word():
    rng = random.Random(7)
    words = [w for w in re.findall(r"[A-Za-z']+", HERSHEY["extract"] + RIFLE["extract"])]
    for _ in range(300):
        text = " ".join(rng.choice(words) for _ in range(rng.randint(3, 40)))
        limit = rng.randint(10, 120)
        out = company_blurb.truncate_at_boundary(text, limit)
        assert len(out) <= limit
        if out != text:
            body = out.rstrip("…")
            assert text.startswith(body)
            assert len(text) == len(body) or not text[len(body)].isalnum(), (text, out)
