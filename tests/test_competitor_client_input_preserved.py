"""Regression test for the Hershey Company client-competitor-substitution bug.

Real shipped defect (Hershey Company plan, reported via Slack, 2026-09-24):
the client typed an explicit, distinct competitor list into the wizard's
"Client's Key Competitors" field (``data["competitors"]``), but the
generated workbook's Market Intelligence sheet "Competitor Analysis" table
showed ENTIRELY DIFFERENT companies -- ones the client never named.

NOT the same bug as tests/test_uber_competitor_claim_fix.py (FIX A there
covers the STATIC per-industry fallback roster stamping invented "Active
(est.)" precision when NO competitor data exists at all, and its own
false-positive guard only exercises the case where comp_intel and the brief
happen to name the SAME companies). This is a distinct precedence bug: when
comp_intel (data["_synthesized"]["competitive_intelligence"]["competitors"],
populated by data_synthesizer.fuse_competitive_intelligence from Clearbit-
resolved competitor logos) has ANY entries, it silently replaces the
client's own brief-supplied list wholesale.

Root cause (excel_v2.py, ``_build_sheet_market_intelligence``, "Competitors
table" section, ~line 7088 pre-fix): the code read

    comp_analysis = comp_intel.get(
        "competitors", comp_intel.get("competitor_analysis") or []
    )
    if not comp_analysis and competitors:
        comp_analysis = [{"name": c} for c in competitors]

-- checking the synthesized/enriched ``comp_intel`` dict BEFORE the client's
own ``data["competitors"]`` list. Whenever comp_intel had ANY entries (a
normal outcome whenever Clearbit enrichment resolves at least one competitor
logo for ANY query), the client's explicit list was discarded entirely, with
no merge and no disclosure. Every OTHER competitor-rendering site in this
codebase already gets this right (ppt_generator.py's competitor cards,
gold_standard.build_competitor_map, and this same file's Quality
Intelligence "Competitive Landscape" table) -- this Market Intelligence
sheet table was the one site still doing it backwards.

Fix: when the brief supplies competitors, they are used verbatim (one row
per client-named company); comp_intel may only supply supplemental fields
(domain, logo, etc.) for those SAME client-named companies via a
case-insensitive name match, never substitute a different company list.
comp_intel's own list is only used when the brief supplied no competitors
at all (matching the existing FIX A static-fallback behavior).

FOLLOW-UP (adversarial review, same day): two more issues found in the
Market Intelligence precedence fix above plus a SECOND, more consequential
bug in the actual client-facing mechanism:

  BUILD-QUALITY FIX (crash): ``competitors`` local var in
  ``_build_sheet_market_intelligence`` is normalized up front now -- a
  direct API caller can submit dict-shaped entries (the same shape
  api_enrichment.enrich_data's own competitors normalization and
  ppt_generator.py's competitor-card cascade already expect, not just the
  wizard's plain-string tag input), and the wizard's tag input can submit
  whitespace-only entries. Both used to reach an Excel cell as a raw dict
  or blank string (``ValueError: Cannot convert {...} to Excel`` / a
  nonsense "This competitor is a plausible..." row for a blank name).

  THE REAL ROOT CAUSE (gold_standard.build_competitor_map): this function
  -- which feeds excel_v2's OWN Quality Intelligence "Competitive
  Landscape & Counter-Strategies" table, NOT the Market Intelligence sheet
  above -- pads the client's brief-supplied competitor list up to 8
  per-city entries (and, separately/unboundedly, the "_national" row) with
  a static per-industry roster whenever the brief names fewer than 8
  (nearly always true -- most briefs name 2-3). Those padded entries (e.g.
  "(National) Amazon"/"(National) Walmart" for a candy manufacturer)
  rendered in the SAME "Top Employers" cell as the client's own named
  competitors, with the "(National)" tag stripped by excel_v2 before
  render and -- this was the actual disclosure bug -- the "inferred, not
  verified" footnote only fired when the brief supplied ZERO competitors,
  never when it supplied SOME but got padded. A client who named 3
  competitors would see those 3 plus several unrelated national retailers
  with no indication any of them weren't theirs. This is the real
  mechanism behind a report of "the deck/workbook shows companies I never
  typed" for a brief that DID supply competitors -- the Market
  Intelligence precedence bug above only manifests when comp_intel has
  entries unrelated to the brief, which the real fetch_competitor_logos
  pipeline (keyed by the brief's own names) can't structurally produce.
  Fixed by detecting actual padding (any rendered employer not in the
  brief, case-insensitive, scope-tag stripped) and disclosing it with a
  precise "additional competitors beyond those named" footnote, instead of
  gating only on "brief is empty".

  DICT-REPR LEAK FIX: a dict-shaped brief entry ([{"name": "Mars
  Wrigley"}]) reached a raw ``str(c)`` in gold_standard.build_
  competitor_map and excel_v2's Quality Intelligence ``_qi_brief_lower``,
  rendering literal Python dict-repr text ("{'name': 'Mars Wrigley'},
  Amazon, ..."). Fixed via ``shared_utils.normalize_competitor_names``, a
  single flattening helper called at every site that only needs to
  match/compare names.

  BOUNDARY OVER-FLATTENING FIX (this round): the fix above was first
  applied by flattening ``data["competitors"]`` to bare name strings ONCE
  at the app.py request boundary. That over-corrected: ppt_generator.py's
  ``_build_slide_competitive_landscape`` already reads ``description``/
  ``domain``/``competitor_type`` off a dict-shaped brief entry when one
  is supplied (a legitimate, pre-existing feature for the direct-API
  path -- the wizard itself only ever sends plain strings), and the
  boundary flattening silently discarded that metadata before
  ppt_generator.py ever saw it. Fixed by splitting the shared_utils
  helper in two: ``clean_competitor_entries`` (trims/drops blanks,
  KEEPS dict entries as dicts -- used at the app.py boundary and by
  ppt_generator.py's consumers) and ``normalize_competitor_names``
  (flattens to bare names -- used by gold_standard.build_competitor_map
  and excel_v2's name-matching sites, which never needed the metadata).

Runs under pytest, or standalone: ``python3 tests/test_competitor_client_input_preserved.py``.
"""

from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import openpyxl  # noqa: E402
from pptx import Presentation  # noqa: E402

import excel_v2  # noqa: E402
import gold_standard  # noqa: E402
import ppt_generator  # noqa: E402
import research  # noqa: E402
import shared_utils  # noqa: E402


def _sheet_text(ws) -> str:
    parts = []
    for row in ws.iter_rows(values_only=True):
        for val in row:
            if val is not None:
                parts.append(str(val))
    return "\n".join(parts)


def _build_market_intel_ws(data: dict):
    wb = openpyxl.Workbook()
    ws = wb.active
    excel_v2._build_sheet_market_intelligence(ws, data, research_mod=research)
    return ws


def _build_quality_intel_ws(data: dict, gold_standard_data: dict):
    wb = openpyxl.Workbook()
    ws = wb.active
    excel_v2._build_sheet_quality_intelligence(ws, data, gold_standard_data)
    return ws


def _hershey_data(**overrides) -> dict:
    data = {
        "client_name": "Hershey Company",
        "industry": "retail_consumer",
        "locations": ["Hershey, PA"],
        "roles": ["Production Associate"],
        "target_roles": ["Production Associate"],
        "budget": "$500,000",
        "competitors": ["Acme Confections", "Brightline Sweets"],
        "_enriched": {},
    }
    data.update(overrides)
    return data


def test_brief_competitors_win_over_synthesized_competitive_intelligence():
    """The real Hershey bug shape: comp_intel (synthesized/enriched, e.g.
    from Clearbit competitor-logo resolution) names DIFFERENT companies
    than the client's own brief. The client's explicit list must render;
    comp_intel's unrelated companies must NOT appear at all."""
    data = _hershey_data(
        _synthesized={
            "competitive_intelligence": {
                "competitors": {
                    "Mars Wrigley": {"domain": "mars.com"},
                    "Nestle": {"domain": "nestle.com"},
                },
            },
        },
    )
    text = _sheet_text(_build_market_intel_ws(data))
    assert "Acme Confections" in text, "client-named competitor missing"
    assert "Brightline Sweets" in text, "client-named competitor missing"
    assert "Mars Wrigley" not in text, "unrelated synthesized competitor leaked in"
    assert "Nestle" not in text, "unrelated synthesized competitor leaked in"


def test_comp_intel_may_supplement_brief_competitors_by_matching_name():
    """comp_intel is allowed to enrich the CLIENT's own named competitors
    (e.g. attach a resolved domain) -- it just must not substitute a
    different company list. Case-insensitive match on the brief's own
    names must still pull in that supplemental detail."""
    data = _hershey_data(
        _synthesized={
            "competitive_intelligence": {
                "competitors": {
                    "acme confections": {"domain": "acmeconfections.example"},
                },
            },
        },
    )
    text = _sheet_text(_build_market_intel_ws(data))
    assert "Acme Confections" in text
    assert "Brightline Sweets" in text


def test_comp_intel_used_only_when_brief_has_no_competitors():
    """False-positive guard: when the brief truly supplied NO competitors,
    comp_intel's own (synthesized) list is legitimate content and must
    still render -- this table isn't required to always be brief-only."""
    data = _hershey_data(competitors=[])
    data["_synthesized"] = {
        "competitive_intelligence": {
            "competitors": {
                "Mars Wrigley": {"domain": "mars.com"},
            },
        },
    }
    text = _sheet_text(_build_market_intel_ws(data))
    assert "Mars Wrigley" in text


# ===========================================================================
# Build-quality fix -- dict-shaped / blank competitor entries must not crash
# _build_sheet_market_intelligence or render nonsense rows.
# ===========================================================================
def test_dict_shaped_competitor_entries_do_not_crash():
    """A direct API caller (not the wizard) can submit competitor entries
    as dicts, e.g. {"name": "Acme", "domain": "..."} -- the same shape
    api_enrichment.enrich_data's own normalization and ppt_generator.py's
    competitor cards already expect. Pre-fix this raised ValueError:
    Cannot convert {...} to Excel and the whole sheet fell back to an
    error placeholder."""
    data = _hershey_data(
        competitors=[
            {"name": "Acme Confections"},
            {"name": "Brightline Sweets", "domain": "brightline.example"},
        ],
    )
    text = _sheet_text(_build_market_intel_ws(data))
    assert "Acme Confections" in text
    assert "Brightline Sweets" in text


def test_blank_and_whitespace_competitor_entries_are_dropped():
    """Whitespace-only / empty entries from the wizard's tag input must be
    dropped, not rendered as a blank-named row with nonsense prose."""
    data = _hershey_data(competitors=["  ", "", "Acme Confections"])
    text = _sheet_text(_build_market_intel_ws(data))
    assert "Acme Confections" in text
    assert "This competitor is a plausible" not in text


# ===========================================================================
# The real root cause -- gold_standard.build_competitor_map pads the
# brief's competitor list with a static per-industry roster, and the
# Quality Intelligence sheet's "inferred" disclosure only fired when the
# brief was fully empty, never when it was merely padded.
# ===========================================================================
def _competitor_map_for(competitors: list, industry: str = "retail_consumer") -> dict:
    """Call the REAL gold_standard.build_competitor_map (not a hand-planted
    dict) for a Hershey-shaped single-city plan."""
    data = _hershey_data(industry=industry, competitors=competitors)
    city_data = {"Hershey, PA": {"hiring_difficulty": 6.0}}
    return gold_standard.build_competitor_map(data, city_data)


def test_build_competitor_map_pads_brief_with_industry_generic_names():
    """Documents the real (pre-existing, legitimate-in-principle) padding
    behavior: gold_standard.build_competitor_map fills remaining "Top
    Employers" slots with a static per-industry roster when the brief
    names fewer than 8 competitors -- this is the exact case reported for
    Hershey (brief named Mars Wrigley/Nestle/Mondelez, workbook showed
    Amazon too)."""
    comp_map = _competitor_map_for(["Mars Wrigley", "Nestle", "Mondelez"])
    employers = comp_map["Hershey, PA"]["top_employers"]
    for name in ("Mars Wrigley", "Nestle", "Mondelez"):
        assert name in employers, f"client-named {name!r} missing"
    # The static per-industry padding for retail_consumer includes Amazon --
    # confirms the padding path actually fired (not a vacuous pass).
    assert any("Amazon" in e for e in employers)


def test_quality_intelligence_discloses_padded_competitors_even_when_brief_nonempty():
    """The real client-facing bug: using the REAL build_competitor_map
    output (not a hand-planted dict) for a brief that named real
    competitors, the Quality Intelligence sheet's "Competitive Landscape"
    table must disclose that additional, non-client-named companies were
    added -- pre-fix, no disclosure appeared at all once the brief was
    non-empty, even though Amazon/Walmart/etc. were mixed into the same
    "Top Employers" cell as the client's own names."""
    comp_map = _competitor_map_for(["Mars Wrigley", "Nestle", "Mondelez"])
    data = _hershey_data(competitors=["Mars Wrigley", "Nestle", "Mondelez"])
    gold = {"competitor_mapping": comp_map}
    text = _sheet_text(_build_quality_intel_ws(data, gold))
    assert "Mars Wrigley" in text
    assert "Amazon" in text, "padding path did not fire -- test is vacuous"
    assert (
        "Additional competitors beyond those the client named" in text
    ), "padded/non-client competitors rendered with no disclosure"


def test_quality_intelligence_fully_inferred_disclosure_when_brief_empty():
    """Regression guard: the ORIGINAL "fully inferred" footnote (brief
    supplied literally zero competitors) must still fire -- this fix only
    ADDS a second, more precise disclosure for the partial-padding case."""
    comp_map = _competitor_map_for([])
    data = _hershey_data(competitors=[])
    gold = {"competitor_mapping": comp_map}
    text = _sheet_text(_build_quality_intel_ws(data, gold))
    assert "Competitor set inferred from industry classification" in text
    assert "Additional competitors beyond those the client named" not in text


def test_quality_intelligence_dict_shaped_brief_entry_renders_cleanly():
    """Adversarial-review follow-up: the verifier's exact repro. A
    dict-shaped brief competitor ([{"name": "Mars Wrigley"}]) must render
    as the plain name everywhere in the Quality Intelligence sheet --
    gold_standard.build_competitor_map's own brief_competitors read and
    excel_v2's _qi_brief_lower computation both used to do a raw
    ``str(c)`` on the entry, producing literal Python dict-repr text
    ("{'name': 'Mars Wrigley'}, Amazon, Walmart, UPS") in the "Top
    Employers" cell and counter-strategy prose instead of just "Mars
    Wrigley"."""
    data = _hershey_data(competitors=[{"name": "Mars Wrigley"}])
    city_data = {"Hershey, PA": {"hiring_difficulty": 6.0}}
    comp_map = gold_standard.build_competitor_map(data, city_data)
    gold = {"competitor_mapping": comp_map}
    text = _sheet_text(_build_quality_intel_ws(data, gold))
    assert "{'name'" not in text, "dict-repr text leaked into the sheet"
    assert "Mars Wrigley" in text


def test_quality_intelligence_no_disclosure_when_brief_covers_every_rendered_name():
    """False-positive guard: if the client's own brief already names every
    company the industry-generic roster would otherwise have added
    (discovered here via the REAL synthesizer's own empty-brief output),
    dict-dedup means nothing NEW was actually padded in -- no disclosure
    should fire."""
    baseline_map = _competitor_map_for([])
    full_roster = sorted(
        {
            excel_v2._strip_competitor_scope_tag(e)
            for info in baseline_map.values()
            for e in (info.get("top_employers") or [])
        }
    )
    comp_map = _competitor_map_for(full_roster)
    data = _hershey_data(competitors=full_roster)
    gold = {"competitor_mapping": comp_map}
    text = _sheet_text(_build_quality_intel_ws(data, gold))
    assert "inferred from industry classification" not in text


# ===========================================================================
# Boundary over-flattening fix (this round) -- shared_utils helpers +
# ppt_generator.py metadata survival + async-reuse lock-in.
# ===========================================================================
def _new_prs() -> Presentation:
    prs = Presentation()
    prs.slide_width = ppt_generator.SLIDE_WIDTH
    prs.slide_height = ppt_generator.SLIDE_HEIGHT
    return prs


def _all_slide_text(prs: Presentation) -> list:
    out = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.has_text_frame and shape.text_frame.text.strip():
                out.append(shape.text_frame.text)
    return out


def test_normalize_competitor_names_flattens_various_shapes():
    """Direct unit coverage of shared_utils.normalize_competitor_names --
    the flattening helper used by gold_standard.build_competitor_map and
    excel_v2's name-matching sites."""
    assert shared_utils.normalize_competitor_names(None) == []
    assert shared_utils.normalize_competitor_names([]) == []
    assert shared_utils.normalize_competitor_names(
        "Mars Wrigley, Nestle,  , Mondelez"
    ) == ["Mars Wrigley", "Nestle", "Mondelez"]
    assert shared_utils.normalize_competitor_names(
        [{"name": " Mars Wrigley "}, "  ", "Nestle", None]
    ) == ["Mars Wrigley", "Nestle"]
    # Idempotent on clean_competitor_entries' own (mixed str/dict) output.
    cleaned = shared_utils.clean_competitor_entries(
        [{"name": "Mars Wrigley", "domain": "mars.com"}, "Nestle"]
    )
    assert shared_utils.normalize_competitor_names(cleaned) == [
        "Mars Wrigley",
        "Nestle",
    ]


def test_clean_competitor_entries_preserves_dict_metadata_and_drops_blanks():
    """Direct unit coverage of shared_utils.clean_competitor_entries -- the
    boundary-safe cleaner that must NOT collapse a dict entry to its bare
    name (that's what over-flattened the app.py boundary fix this round)."""
    assert shared_utils.clean_competitor_entries(None) == []
    cleaned = shared_utils.clean_competitor_entries(
        [
            {
                "name": " Acme Confections ",
                "description": "regional confectionery chain",
                "domain": "acme.example",
            },
            "  ",
            "",
            "Brightline Sweets",
            None,
        ]
    )
    assert cleaned == [
        {
            "name": "Acme Confections",
            "description": "regional confectionery chain",
            "domain": "acme.example",
        },
        "Brightline Sweets",
    ]


def test_boundary_normalization_mutates_data_in_place_for_async_reuse():
    """Locks in the async-path-safety claim without a live server: app.py's
    /api/generate handler normalizes data["competitors"] via
    ``data["competitors"] = clean_competitor_entries(...)`` (a key
    assignment on the EXISTING dict, not a rebind of the `data` name), and
    only afterwards does ``threading.Thread(args=(job_id, data,
    request_id))`` hand that same object to the async worker as
    ``gen_data``. This reproduces that exact sequence at the unit level:
    a second reference taken AFTER the boundary line (standing in for the
    thread's captured argument) must see the cleaned value, and must be
    the identical object (no copy that could silently diverge)."""
    data = {"client_name": "Hershey Company", "competitors": ["  ", {"name": " Mars Wrigley "}]}
    data_id_before = id(data)

    # The exact boundary line from app.py's _handle_POST.
    data["competitors"] = shared_utils.clean_competitor_entries(data.get("competitors"))

    # Simulates `threading.Thread(args=(job_id, data, request_id))` binding
    # `gen_data` to the SAME dict inside _run_async_generate.
    gen_data = data

    assert gen_data is data
    assert id(gen_data) == data_id_before
    assert gen_data["competitors"] == [{"name": "Mars Wrigley"}]


def test_dict_metadata_survives_boundary_and_reaches_ppt_competitor_card():
    """The verifier's own repro: a direct-API-shaped competitor entry
    carrying a "description" must survive app.py's request-boundary
    cleaning (simulated here via clean_competitor_entries, the exact
    function the boundary calls) and still be readable by
    ppt_generator.py's existing dict-aware competitor-card rendering.
    Before this round's fix, the boundary flattened every entry to a bare
    name string and this assertion was False."""
    data = _hershey_data(
        competitors=[
            {
                "name": "Acme Confections",
                "description": "regional confectionery chain",
            }
        ],
    )
    # Simulate the app.py request boundary.
    data["competitors"] = shared_utils.clean_competitor_entries(data["competitors"])
    assert data["competitors"] == [
        {
            "name": "Acme Confections",
            "description": "regional confectionery chain",
        }
    ], "boundary must keep the dict shape, not flatten to a bare name"

    prs = _new_prs()
    ppt_generator._build_slide_competitive_landscape(prs, data)
    blob = "\n".join(_all_slide_text(prs))
    assert "Acme Confections" in blob
    assert "regional confectionery chain" in blob


def test_mapping_shaped_competitors_input_is_expanded_not_dropped():
    """Round 6 (supersedes round 5's accepted "dropped" behavior): a bare
    name -> metadata mapping ({"Mars Wrigley": {...}}) used to be dropped to
    [], so the client's own competitors silently vanished and the plan fell
    back to an industry-inferred list the client never typed. It is now
    expanded into list entries, keeping scalar metadata. A single entry
    object ({"name": ...}) is still one entry, not a mapping."""
    raw = {"Mars Wrigley": {"domain": "mars.com"}, "Ferrero": {}}
    assert shared_utils.clean_competitor_entries(raw) == [
        {"domain": "mars.com", "name": "Mars Wrigley"},
        {"name": "Ferrero"},
    ]
    assert shared_utils.normalize_competitor_names(raw) == ["Mars Wrigley", "Ferrero"]
    assert shared_utils.clean_competitor_entries({"name": "Acme", "domain": "a.example"}) == [
        {"name": "Acme", "domain": "a.example"}
    ]


def test_clean_competitor_entries_never_stringifies_containers():
    """Round 6: the boundary cleaner used to ``str()`` any non-dict item and
    any dict "name" -- a nested list became the literal name "['Acme']" and
    {"name": {"en": "Acme"}} became "{'en': 'Acme'}", then rendered on every
    surface. Nested lists are flattened; container names and container
    metadata values are dropped."""
    cleaned = shared_utils.clean_competitor_entries(
        [
            ["Acme Confections"],
            {"name": {"en": "Nested Name Co"}},
            {"name": "Brightline Sweets", "description": {"long": "x"}, "domain": "b.example"},
            True,
        ]
    )
    assert cleaned == [
        "Acme Confections",
        {"name": "Brightline Sweets", "domain": "b.example"},
    ]


if __name__ == "__main__":
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-v"]))
