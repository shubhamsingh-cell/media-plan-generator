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
    # review round: the same France plan with nothing excluded, with Indeed
    # excluded, with LinkedIn excluded; a UK plan with "Total Jobs" excluded;
    # a mixed US + UK plan.
    "fr_plain": {
        "client_name": "Intl Fixture FR",
        "locations": ["Paris, France", "Lyon, France"],
        "custom_countries": ["FR"],
        "competitors": ["DHL"],
    },
    "fr_indeed": {
        "client_name": "Intl Fixture FR",
        "locations": ["Paris, France", "Lyon, France"],
        "custom_countries": ["FR"],
        "competitors": ["DHL"],
        "excluded_publishers": ["indeed.com"],
    },
    "fr_linkedin": {
        "client_name": "Intl Fixture FR",
        "locations": ["Paris, France", "Lyon, France"],
        "custom_countries": ["FR"],
        "competitors": ["DHL"],
        "excluded_publishers": ["LinkedIn"],
    },
    "uk_plain": {
        "client_name": "Intl Fixture UK",
        "locations": ["London, United Kingdom", "Manchester, United Kingdom"],
        "custom_countries": ["UK"],
        "competitors": ["DHL"],
    },
    "uk_totaljobs": {
        "client_name": "Intl Fixture UK",
        "locations": ["London, United Kingdom", "Manchester, United Kingdom"],
        "custom_countries": ["UK"],
        "competitors": ["DHL"],
        "excluded_publishers": ["Total Jobs"],
    },
    # round 3: a US plan excluding a DIFFERENT Indeed product; an AU plan
    # excluding Seek whose client name contains it; Spain + Italy (no board data)
    "us_hiringlab": {
        "client_name": "Intl Fixture US",
        "locations": ["Houston, TX", "Dallas, TX"],
        "target_region": "us_only",
        "custom_countries": [],
        "competitors": ["DHL"],
        "excluded_publishers": ["Indeed Hiring Lab", "Indeed Flex"],
    },
    "au_plain": {
        "client_name": "Seek Hiring Co",
        "locations": ["Sydney, Australia", "Melbourne, Australia"],
        "custom_countries": ["AU"],
        "competitors": ["DHL"],
    },
    "au_seek": {
        "client_name": "Seek Hiring Co",
        "locations": ["Sydney, Australia", "Melbourne, Australia"],
        "custom_countries": ["AU"],
        "competitors": ["DHL"],
        "excluded_publishers": ["Seek"],
    },
    "es_it": {
        "client_name": "Intl Fixture EU",
        "locations": ["Madrid, Spain", "Rome, Italy"],
        "custom_countries": ["ES", "IT"],
        "competitors": ["DHL"],
    },
    "multi": {
        "client_name": "Intl Fixture Multi",
        "locations": [
            "Paris, France",
            "Berlin, Germany",
            "Istanbul, Turkey",
            "Sydney, Australia",
        ],
        "custom_countries": ["FR", "DE", "TR", "AU"],
        "competitors": ["DHL"],
    },
    "mixed": {
        "client_name": "Intl Fixture Mixed",
        "locations": ["Dallas, TX", "London, United Kingdom"],
        "custom_countries": ["US", "UK"],
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

                def _walk(shapes):
                    for sh in shapes:
                        yield sh
                        if sh.shape_type == 6:  # group: read what it contains
                            yield from _walk(sh.shapes)

                for i, slide in enumerate(prs.slides, 1):
                    for shape in _walk(slide.shapes):
                        if shape.has_text_frame and shape.text_frame.text.strip():
                            deck.append((f"slide{i}", shape.text_frame.text))
                        if getattr(shape, "has_table", False) and shape.has_table:
                            for row in shape.table.rows:
                                for cell in row.cells:
                                    if cell.text.strip():
                                        deck.append((f"slide{i}:table", cell.text))
                    if slide.has_notes_slide:
                        notes = slide.notes_slide.notes_text_frame
                        if notes is not None and notes.text.strip():
                            deck.append((f"slide{i}:notes", notes.text))
    return xlsx, deck


@pytest.fixture(scope="module")
def bundles(live_port):  # noqa: F811
    out: Dict[str, Tuple[Text, Text]] = {}
    for n, (tag, over) in enumerate(_PLANS.items(), 1):
        # one client name per plan: the handler de-duplicates a repeat name
        payload = dict(_BASE, **over)
        payload["client_name"] = f"{payload['client_name']} run {n}"
        status, raw = post_json(live_port, "/api/generate", payload, timeout=300.0)
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


# A board named as the publisher of a figure: "(Indeed Mobile Report 2025)",
# "Publisher: Indeed", "Indeed Hiring Lab", "LinkedIn Benchmarks", "N LinkedIn
# job postings". Written independently of publisher_filter on purpose.
_CITATION_RE = re.compile(
    r"\([^()]*(?:20\d\d|report|study|survey|research|lab|index|benchmark|data)[^()]*\)"
    r"|Publisher:\s*\w+|Title:[^;]*"
    r"|\b(?:indeed|linkedin)\s+(?:(?:\w+\s+){0,2}(?:reports?|lab|benchmarks?|job postings|talent solutions))\b",
    re.I,
)


def _hits(rows: Text, pattern: str, flags: int = 0) -> List[Tuple[str, str]]:
    rx = re.compile(pattern, flags)
    return [(w, t[:120]) for w, t in rows if rx.search(t)]


# ---------------------------------------------------------------------------
# B1 -- client exclusions
# ---------------------------------------------------------------------------


def test_fr_exclusions_leave_no_recommendation_and_the_plan_discloses_them(bundles):
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
        # the disclosure sentence is the one place the names may appear as
        # such; a CITATION (who published a figure) is provenance, not a
        # recommendation, and stays (see test_exclusion_keeps_every_citation)
        stripped = re.sub(r"Excluded at client request:?\s*[^\n]*", "", text)
        stripped = _CITATION_RE.sub("", stripped)
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
    # a citation (who published a cited study) is provenance and stays
    rows = [(w, _CITATION_RE.sub("", t)) for w, t in rows]
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


# ---------------------------------------------------------------------------
# Review round: sweep precision, provenance, market scope, compounds
# ---------------------------------------------------------------------------


def _citations(rows: Text, board: str) -> List[str]:
    """Every citation naming ``board`` (a parenthetical with a year / report
    noun, a Publisher: line, a "<board> Benchmarks" header), as printed."""
    found: List[str] = []
    for where, text in rows:
        for m in re.finditer(
            rf"\((?=[^()]*(?:20\d\d|report|study|survey|research|lab|index|benchmark|data))"
            rf"[^()]*\b{board}\b[^()]*\)|Publisher:\s*{board}\b|{board}\s+Benchmarks",
            text,
            re.I,
        ):
            found.append(f"{where.split('!')[0]}|{m.group(0).lower()}")
    return found


def test_exclusion_keeps_every_citation(bundles):
    plain = _everything(bundles["fr_plain"])
    for tag, board in (("fr_indeed", "Indeed"), ("fr_linkedin", "LinkedIn")):
        before = _citations(plain, board)
        after = _citations(_everything(bundles[tag]), board)
        assert before, (board, "fixture should carry citations")
        missing = [c for c in before if c not in after]
        assert not missing, (tag, missing)


def test_excluding_linkedin_keeps_its_data_attribution(bundles):
    rows = _everything(bundles["fr_linkedin"])
    text = "\n".join(t for _, t in rows)
    assert re.search(r"LINKEDIN BENCHMARKS", text)
    assert re.search(r"job postings across \d+ countries", text)


def test_a_compound_buy_loses_only_the_excluded_member(bundles):
    xlsx, _ = bundles["fr_linkedin"]
    recs = [t for w, t in xlsx if w.startswith("Channel Recommendations!")]
    platforms = [t for t in recs if "Meta" in t and "TikTok" in t]
    assert platforms, recs
    assert not [t for t in platforms if re.search(r"linkedin", t, re.I)], platforms
    # the Social row still has its channel, not an empty cell
    assert any(t == "Meta / TikTok Ads" for t in platforms), platforms
    assert any(t.startswith("Buy via Meta / TikTok Ads.") for t in platforms), platforms


def test_excluding_total_jobs_removes_totaljobs_not_the_word_total(bundles):
    plain, excl = _everything(bundles["uk_plain"]), _everything(bundles["uk_totaljobs"])

    def total_labels(rows: Text) -> List[str]:
        return sorted(
            f"{w.split('!')[0]}|{t}"
            for w, t in rows
            if re.fullmatch(
                r"\s*(?:Campaign |Grand |Total )?Total(?: Budget| Investment)?\s*", t
            )
        )

    assert total_labels(plain), "fixture should carry Total labels"
    assert total_labels(excl) == total_labels(plain)
    # the citation that happens to start with "Total" survives
    assert _hits(plain, r"Total Economic Impact") == _hits(
        excl, r"Total Economic Impact"
    )
    # ...and Totaljobs itself is gone from every board list and the deck
    assert _hits(plain, r"Totaljobs")
    leftovers = [
        w
        for w, t in excl
        if re.search(
            r"total\s*jobs", re.sub(r"Excluded at client request:?[^\n]*", "", t), re.I
        )
    ]
    assert not leftovers, leftovers


def test_uk_exit_does_not_remove_ziprecruiter_from_a_dallas_plan(bundles):
    xlsx, _ = bundles["mixed"]
    zip_rows = [
        t
        for w, t in xlsx
        if w.startswith("Channels & Strategy!") and t == "ZipRecruiter Sponsored"
    ]
    assert zip_rows, "Dallas market still runs ZipRecruiter"
    assert _hits(_everything(bundles["mixed"]), r"ZipRecruiter")


def test_deck_tables_and_notes_carry_no_excluded_board(bundles):
    pattern = {
        "fr_excl": r"hello\s*work|regionsjob|indeed",
        "mx": r"\bOCC\b",
        "uk_totaljobs": r"total\s*jobs",
    }
    for tag, rx in pattern.items():
        _, deck = bundles[tag]
        extra = [
            (w, t) for w, t in deck if w.endswith(":table") or w.endswith(":notes")
        ]
        for where, text in extra:
            stripped = _CITATION_RE.sub("", text)
            stripped = re.sub(r"Excluded at client request:?[^\n]*", "", stripped)
            assert not re.search(rx, stripped, re.I), (tag, where, text[:100])


def test_deck_band_says_how_many_markets_it_left_out_and_renders_the_dagger(bundles):
    _, deck = bundles["multi"]
    text = "\n".join(t for _, t in deck)
    assert "+1 more market" in text, text[-400:]
    assert "Kariyer.net \u2020" in text  # Istanbul resolves to Turkey, not the US
    assert "relationship unverified \u2014 confirm before quoting" in text
    xlsx, _ = bundles["multi"]
    countries = [t for w, t in xlsx if re.fullmatch(r"Intl Benchmarks!B\d+", w)]
    assert "Turkey" in countries and "Australia" in countries


def test_excluding_another_indeed_product_keeps_indeed_sponsored_jobs(bundles):
    xlsx, _ = bundles["us_hiringlab"]
    assert [
        t
        for w, t in xlsx
        if w.startswith("Channels & Strategy!") and t == "Indeed Sponsored Jobs"
    ]
    recs = [t for w, t in xlsx if w.startswith("Channel Recommendations!")]
    assert any("Indeed Sponsored Jobs" in t for t in recs), recs


def test_excluding_seek_removes_the_board_not_the_word_or_the_client_name(bundles):
    plain, excl = _everything(bundles["au_plain"]), _everything(bundles["au_seek"])
    assert _hits(plain, r"\bSeek\b"), "fixture should name the board"
    # the board is gone from every list ...
    lists = [
        t
        for w, t in excl
        if w.startswith(("Channels & Strategy!D", "Intl Benchmarks!D"))
    ]
    assert not [t for t in lists if re.search(r"\bSeek\b", t)], lists

    # ... the client's own name is intact everywhere it appears ...
    def with_client(rows):
        return sorted(w.split("!")[0] for w, t in rows if "Seek Hiring Co" in t)

    assert with_client(excl) == with_client(plain)

    # ... and lower-case prose ("seek", "job seekers") is untouched
    def lower(rows):
        return sorted(w.split("!")[0] for w, t in rows if re.search(r"\bseek", t))

    assert lower(excl) == lower(plain)


def test_slide4_says_which_markets_have_no_board_data(bundles):
    _, deck = bundles["es_it"]
    text = "\n".join(t for _, t in deck)
    assert "No board data for: Spain, Italy" in text
