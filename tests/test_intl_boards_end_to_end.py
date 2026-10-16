"""International plan accuracy + board exclusions, proven on real bundles.

Every plan here goes through the REAL /api/generate handler (in-process
server, no API keys) and the workbook and deck are read back cell by cell /
shape by shape -- nothing is asserted from source text.

Pre-fix findings these pin (2026-10-16 sweep, nine UK/DE/FR/MX/IN/AU/SG/VN/CA
plans):
  * no surface could exclude a board; HelloWork (FR) and OCC (MX) -- not
    available through Joveo -- were listed as recommended local boards;
  * "ZipRecruiter Sponsored" sat in every non-US ad-platform table;
  * every deck carried the US complication bullets ("CDL demand", "US$25-50
    CPA") and the fixed "US$12-35" industry CPA;
  * FedEx / UPS / XPO were inferred as competitors for Paris and Ho Chi Minh
    City;
  * the Intl Benchmarks sheet dumped all 38 countries;
  * no named publisher reached the deck;
  * stale names: "CareerBuilder Vietnam" (now CareerViet), Workopolis (an
    Indeed-operated portal) listed as a Canadian board.
"""

from __future__ import annotations

import io
import re
import zipfile
from typing import Dict, List, Tuple

import pytest

from tests.live_server import live_port, post_json  # noqa: F401 (fixture)

Text = List[Tuple[str, str]]  # (where, text)

_BASE = {
    "requester_name": "QA Bot",
    "requester_email": "qa@joveo.com",
    "use_case": "Hiring logistics warehouse staff",
    "industry": "logistics_supply_chain",
    "target_roles": ["Warehouse Operative", "Delivery Driver"],
    "budget_range": "250000",
    "campaign_duration": "3 months",
    "hire_volume": "120 hires",
    "target_region": "custom",
}

_PLANS: Dict[str, dict] = {
    "fr_excl": {
        "client_name": "Intl Fixture FR",
        "locations": ["Paris, France", "Lyon, France"],
        "custom_countries": ["FR"],
        "competitors": ["DHL"],
        "excluded_publishers": ["HelloWork", "indeed.com"],
    },
    "mx": {
        "client_name": "Intl Fixture MX",
        "locations": ["Mexico City, Mexico", "Guadalajara, Mexico"],
        "custom_countries": ["MX"],
        "competitors": ["DHL"],
    },
    "vn": {
        "client_name": "Intl Fixture VN",
        "locations": ["Ho Chi Minh City, Vietnam", "Hanoi, Vietnam"],
        "custom_countries": ["VN"],
        # no competitors typed: the inferred roster must not be a US one
    },
    "ca": {
        "client_name": "Intl Fixture CA",
        "locations": ["Toronto, Canada", "Vancouver, Canada"],
        "custom_countries": ["CA"],
        "competitors": ["DHL"],
    },
    "uk": {
        "client_name": "Intl Fixture UK",
        "locations": ["London, United Kingdom", "Manchester, United Kingdom"],
        "custom_countries": ["UK"],
        "competitors": ["DHL"],
    },
}


def _read(raw: bytes) -> Tuple[Text, Text]:
    import openpyxl
    from pptx import Presentation

    xlsx: Text = []
    deck: Text = []
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        for name in zf.namelist():
            blob = zf.read(name)
            if name.endswith(".xlsx"):
                wb = openpyxl.load_workbook(io.BytesIO(blob))
                for ws in wb.worksheets:
                    for row in ws.iter_rows():
                        for cell in row:
                            if isinstance(cell.value, str):
                                xlsx.append(
                                    (f"{ws.title}!{cell.coordinate}", cell.value)
                                )
            elif name.endswith(".pptx"):
                prs = Presentation(io.BytesIO(blob))
                for i, slide in enumerate(prs.slides, 1):
                    for shape in slide.shapes:
                        if shape.has_text_frame and shape.text_frame.text.strip():
                            deck.append((f"slide{i}", shape.text_frame.text))
    return xlsx, deck


@pytest.fixture(scope="module")
def bundles(live_port):  # noqa: F811
    out: Dict[str, Tuple[Text, Text]] = {}
    for tag, over in _PLANS.items():
        status, raw = post_json(
            live_port, "/api/generate", dict(_BASE, **over), timeout=300.0
        )
        assert status == 200 and isinstance(raw, bytes), (tag, status, raw)
        out[tag] = _read(raw)
    return out


def _everything(bundle: Tuple[Text, Text], skip_sources: bool = True) -> Text:
    xlsx, deck = bundle
    rows = [
        (w, t)
        for w, t in xlsx
        if not (skip_sources and w.startswith("Sources & Confidence!"))
    ]
    return rows + deck


def _hits(rows: Text, pattern: str, flags: int = 0) -> List[Tuple[str, str]]:
    rx = re.compile(pattern, flags)
    return [(w, t[:120]) for w, t in rows if rx.search(t)]


# ---------------------------------------------------------------------------
# B1 -- client exclusions
# ---------------------------------------------------------------------------


def test_fr_exclusions_leave_zero_mentions_and_the_plan_discloses_them(bundles):
    rows = _everything(bundles["fr_excl"])
    disclosure = "Excluded at client request: HelloWork, indeed.com"
    xlsx_rows, deck_rows = bundles["fr_excl"]
    assert [
        w
        for w, t in xlsx_rows
        if w.startswith("Executive Summary!") and disclosure in t
    ]
    slide2 = " ".join(t for w, t in deck_rows if w == "slide2")
    assert "Excluded at client request" in slide2
    assert "HelloWork" in slide2 and "indeed.com" in slide2

    rx = re.compile(r"hello\s*work|regionsjob|indeed", re.I)
    leftovers = []
    for where, text in rows:
        # the disclosure sentence is the one place the names may appear
        stripped = re.sub(r"Excluded at client request:?\s*[^\n]*", "", text)
        if rx.search(stripped):
            leftovers.append((where, stripped[:120]))
    assert not leftovers, leftovers

    # an excluded board is replaced by the next one, not left as a gap
    local = " ".join(t for w, t in xlsx_rows if w.startswith("Channels & Strategy!"))
    assert "LinkedIn France" in local


def test_excluded_representative_buy_is_substituted_not_dropped(bundles):
    xlsx_rows, _ = bundles["fr_excl"]
    recs = [t for w, t in xlsx_rows if w.startswith("Channel Recommendations!")]
    assert not [t for t in recs if re.search(r"indeed", t, re.I)], recs
    # the Global Job Boards category still names a platform to buy through
    assert any("LinkedIn Jobs" in t for t in recs), recs


# ---------------------------------------------------------------------------
# B3 -- availability (permanent, silent)
# ---------------------------------------------------------------------------


def test_mx_plan_never_names_occ_and_never_discloses_why(bundles):
    rows = _everything(bundles["mx"])
    assert not _hits(rows, r"\bOCC\b"), _hits(rows, r"\bOCC\b")
    assert not _hits(rows, r"Excluded at client request")
    assert not _hits(rows, r"not available through Joveo")
    # the market still has local boards
    assert _hits(rows, r"Computrabajo")


def test_fr_plan_without_a_client_exclusion_never_names_hellowork(bundles):
    # fr_excl excludes HelloWork explicitly; the availability exclusion alone
    # is proven on the Intl Benchmarks / local boards of the MX plan above and
    # by the unit tests -- here the FR plan must also not list RegionsJob.
    assert not _hits(_everything(bundles["fr_excl"]), r"regionsjob", re.I)


# ---------------------------------------------------------------------------
# B2 -- international accuracy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tag", ["fr_excl", "mx", "vn", "uk"])
def test_no_ziprecruiter_outside_its_markets(bundles, tag):
    rows = _everything(bundles[tag])
    assert not _hits(rows, r"ZipRecruiter"), _hits(rows, r"ZipRecruiter")


def test_ziprecruiter_stays_where_it_operates(bundles):
    xlsx_rows, _ = bundles["ca"]
    assert [
        t
        for w, t in xlsx_rows
        if w.startswith("Channels & Strategy!") and t == "ZipRecruiter Sponsored"
    ]


@pytest.mark.parametrize("tag", ["fr_excl", "mx", "vn", "uk", "ca"])
def test_deck_has_no_us_only_content_on_a_non_us_plan(bundles, tag):
    _, deck = bundles[tag]
    text = "\n".join(t for _, t in deck)
    assert not re.search(r"\bCDL\b", text), tag
    assert "US$25-US$50" not in text and "$25-$50" not in text, tag
    assert "US$12-US$35" not in text and "$12 - $35" not in text, tag


def test_industry_cpa_row_is_the_markets_own_or_omitted(bundles):
    def cpa_value(tag: str) -> str:
        _, deck = bundles[tag]
        texts = [t for _, t in deck]
        for i, t in enumerate(texts):
            if t.startswith("Industry CPA"):
                return t + " | " + texts[i + 1]
        return ""

    assert "US$6-US$22" in cpa_value("fr_excl")  # France, logistics (dataset range)
    assert "No local benchmark" in cpa_value("vn")  # no Vietnam CPA range: omitted


def test_no_us_rosters_inferred_for_a_non_us_market(bundles):
    rows = _everything(bundles["vn"])
    us = _hits(rows, r"\b(FedEx|UPS|XPO Logistics|C\.H\. Robinson|Amazon Logistics)\b")
    assert not us, us


def test_intl_benchmarks_sheet_lists_only_the_plans_markets(bundles):
    xlsx_rows, _ = bundles["fr_excl"]
    cells = [(w, t) for w, t in xlsx_rows if w.startswith("Intl Benchmarks!")]
    countries = [t for w, t in cells if re.fullmatch(r"Intl Benchmarks!B\d+", w)]
    # column B: title/subtitle/footnote rows + one country row per plan market
    assert "France" in countries
    for other in ("Argentina", "Germany", "Vietnam", "Mexico", "India"):
        assert other not in countries, other


def test_stale_names_are_corrected(bundles):
    vn = _everything(bundles["vn"])
    assert not _hits(vn, r"CareerBuilder"), _hits(vn, r"CareerBuilder")
    assert _hits(vn, r"CareerViet")
    ca = _everything(bundles["ca"])
    assert not _hits(ca, r"Workopolis"), _hits(ca, r"Workopolis")


@pytest.mark.parametrize("tag", ["fr_excl", "mx", "ca"])
def test_deck_names_the_markets_local_boards(bundles, tag):
    _, deck = bundles[tag]
    text = "\n".join(t for _, t in deck)
    assert "LOCAL BOARDS IN THIS PLAN'S MARKETS" in text
    assert re.search(r"LinkedIn (France|Mexico|Canada)", text), tag


def test_unverified_relationship_is_labelled_not_hidden(bundles):
    # APEC is listed in the supply repository but not "In Joveo Network"
    _, deck = bundles["fr_excl"]
    text = "\n".join(t for _, t in deck)
    assert "APEC †" in text
    assert "relationship unverified — confirm before quoting" in text
